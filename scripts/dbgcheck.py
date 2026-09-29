#!/usr/bin/env python3
"""Local script client for the Go debug protocol server.

It speaks newline-delimited JSON over TCP or a Unix domain socket, so no
GUI or real process injection is involved. Two scenarios are verified:

  * abnormal executor exit: the terminated event carries a non-zero code
    and later control requests fail with ProcessExited;
  * duplicate continue: continuing a running/old pause is rejected with
    NotPaused / StalePause and the session still reaches its next stop.

Exit status is non-zero if any expectation fails.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path


class ProtocolError(Exception):
    pass


class Client:
    def __init__(self, reader, writer, close_cb=None):
        self.reader = reader
        self.writer = writer
        self.close_cb = close_cb
        self.next_id = 0
        self.events: list[dict] = []
        self._lock = threading.Lock()
        self._cv = threading.Condition()
        self._replies: dict[int, dict] = {}
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    @classmethod
    def connect(cls, addr: str, timeout: float = 5.0) -> "Client":
        family = socket.AF_UNIX if addr.startswith("/") else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        if family == socket.AF_UNIX:
            sock.connect(addr)
        else:
            host, port = addr.split(":")
            sock.connect((host, int(port)))
        return cls(sock.makefile("rb", buffering=0), sock.makefile("wb", buffering=0), sock.close)

    @classmethod
    def spawn_stdio(cls, server_bin: str) -> tuple["Client", subprocess.Popen]:
        proc = subprocess.Popen(
            [server_bin, "-stdio"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        client = cls(proc.stdout, proc.stdin)

        def shutdown() -> None:
            if proc.stdin is not None:
                proc.stdin.close()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

        client.close_cb = shutdown
        return client, proc

    def _read_loop(self) -> None:
        try:
            for line in self.reader:
                msg = json.loads(line.decode())
                if msg.get("type") == "event":
                    with self._cv:
                        self.events.append(msg)
                        self._cv.notify_all()
                else:
                    with self._cv:
                        self._replies[msg["requestId"]] = msg
                        self._cv.notify_all()
        except OSError:
            pass

    def request(self, command: str, arguments: dict | None = None) -> dict:
        with self._lock:
            self.next_id += 1
            req_id = self.next_id
        payload = json.dumps(
            {"id": req_id, "command": command, "arguments": arguments or {}}
        ) + "\n"
        self.writer.write(payload.encode())
        self.writer.flush()
        deadline = time.time() + 5
        with self._cv:
            while req_id not in self._replies:
                remaining = deadline - time.time()
                if remaining <= 0 or not self._cv.wait(timeout=remaining):
                    raise ProtocolError(f"timeout waiting for reply to {command}")
            return self._replies[req_id]

    def expect_ok(self, command: str, arguments: dict | None = None) -> dict:
        reply = self.request(command, arguments)
        if not reply.get("success"):
            raise ProtocolError(
                f"{command} unexpectedly failed: {json.dumps(reply.get('error'))}"
            )
        return reply.get("body", {})

    def expect_error(self, command: str, arguments: dict, *codes: str) -> dict:
        reply = self.request(command, arguments)
        if reply.get("success"):
            raise ProtocolError(f"{command} unexpectedly succeeded: {reply}")
        code = (reply.get("error") or {}).get("code")
        if code not in codes:
            raise ProtocolError(
                f"{command} failed with {code}, expected one of {codes}"
            )
        return reply["error"]

    def wait_event(
        self, event: str, timeout: float = 5.0, after: int = 0
    ) -> dict:
        deadline = time.time() + timeout
        with self._cv:
            while True:
                for msg in self.events[after:]:
                    if msg.get("event") == event:
                        return msg
                remaining = deadline - time.time()
                if remaining <= 0 or not self._cv.wait(timeout=remaining):
                    raise ProtocolError(f"timeout waiting for event {event}")

    def drain_after(self, count: int) -> list[dict]:
        with self._cv:
            return list(self.events[count:])

    def event_count(self) -> int:
        with self._cv:
            return len(self.events)

    def close(self) -> None:
        if self.close_cb is not None:
            self.close_cb()


ABORT_PROGRAM = [
    {"line": 1, "op": "set", "var": "x", "value": 3},
    {"line": 2, "op": "abort", "code": 9, "message": "kaboom"},
]

LONG_PROGRAM = [{"line": 1, "op": "set", "var": "x", "value": 0}]
for line in range(2, 120):
    LONG_PROGRAM.append(
        {"line": line, "op": "add", "var": "x", "a": "x", "b": "1"}
    )
LONG_PROGRAM.append({"line": 120, "op": "exit", "code": 0})


def check_abnormal_exit(client: Client) -> None:
    client.expect_ok("initialize")
    client.expect_ok("launch", {"executor": "virtual", "program": ABORT_PROGRAM})
    stopped = client.wait_event("stopped")
    pause_id = stopped["data"]["pauseId"]
    events_before = len(client.events)

    client.expect_ok("continue", {"pauseId": pause_id, "action": "continue"})
    terminated = client.wait_event("terminated", after=events_before)
    data = terminated.get("data", {})
    if data.get("exitCode") != 9 or data.get("message") != "kaboom":
        raise ProtocolError(f"bad terminated payload: {data}")

    err = client.expect_error("continue", {"pauseId": pause_id}, "ProcessExited")
    if err.get("exitCode") != 9:
        raise ProtocolError("ProcessExited error must carry the exit code")
    client.expect_error("variables", {"pauseId": pause_id}, "ProcessExited")

    after = client.drain_after(events_before)
    kinds = [m.get("event") for m in after]
    if kinds != ["continued", "terminated"]:
        raise ProtocolError(f"unexpected event ordering after resume: {kinds}")
    print("ok: abnormal exit ends the session and all waiting control")


def check_duplicate_continue(client: Client) -> None:
    client.expect_ok("initialize")
    body = client.expect_ok(
        "launch",
        {
            "executor": "virtual",
            "program": LONG_PROGRAM,
            # Park while running at line 59; line 60 still has the breakpoint.
            "holdBeforeLine": 59,
        },
    )
    stopped = client.wait_event("stopped")
    first_pause = stopped["data"]["pauseId"]

    # Breakpoints are only accepted on real instruction lines.
    bp = client.expect_ok("setBreakpoints", {"lines": [60, 777]})
    if bp.get("verified") != [60] or bp.get("rejected") != [777]:
        raise ProtocolError(f"breakpoint verification wrong: {bp}")

    # An old/unknown pause generation must never touch the current pause.
    err = client.expect_error(
        "continue", {"pauseId": first_pause + 100}, "StalePause"
    )
    if err.get("currentPauseId") != first_pause:
        raise ProtocolError("StalePause must report currentPauseId")

    client.expect_ok("continue", {"pauseId": first_pause})
    client.wait_event("continued")
    # The hold is at the line-60 pause decision, before the stopped event is
    # emitted and while status is still running.
    time.sleep(0.05)

    # Duplicate continue while running must not enqueue a second resume.
    client.expect_error("continue", {"pauseId": first_pause}, "NotPaused")
    # Variables while running fail immediately instead of returning a mixed
    # snapshot or blocking until the next pause.
    client.expect_error("variables", {"pauseId": first_pause}, "NotPaused")

    client.expect_ok("releaseHold")
    next_stop = client.wait_event(
        "stopped", after=client.event_count()
    )
    if next_stop["data"].get("pauseId") == first_pause:
        raise ProtocolError("new pause must have a new generation id")
    if next_stop["data"].get("line") != 60 or next_stop["data"].get("reason") != "breakpoint":
        raise ProtocolError(f"unexpected stop: {next_stop['data']}")

    # The old generation cannot operate on the new pause.
    client.expect_error(
        "variables", {"pauseId": first_pause}, "StalePause"
    )
    new_pause = next_stop["data"]["pauseId"]
    vars_body = client.expect_ok("variables", {"pauseId": new_pause})
    if vars_body["variables"].get("x") != 58 or vars_body.get("line") != 60:
        raise ProtocolError(f"snapshot at line 60 wrong: {vars_body}")

    client.expect_ok("continue", {"pauseId": new_pause})
    terminated = client.wait_event("terminated")
    if terminated["data"].get("exitCode") != 0:
        raise ProtocolError("clean program should exit 0")

    # Duplicate continue after termination.
    client.expect_error(
        "continue", {"pauseId": new_pause}, "ProcessExited"
    )
    print("ok: duplicate/stale continue rejected and pause generations enforced")


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug protocol script client")
    parser.add_argument(
        "--addr",
        help="existing server address: TCP host:port or absolute Unix socket path",
    )
    parser.add_argument(
        "--server",
        default=None,
        help="path to dbgserver binary; defaults to bin/dbgserver for --stdio",
    )
    args = parser.parse_args()

    addr = args.addr
    server_bin = args.server or str(
        Path(__file__).resolve().parents[1] / "bin" / "dbgserver"
    )

    failures = 0
    procs: list[subprocess.Popen] = []
    for check in (check_abnormal_exit, check_duplicate_continue):
        proc = None
        client = None
        try:
            if addr is not None:
                client = Client.connect(addr)
            else:
                client, proc = Client.spawn_stdio(server_bin)
                procs.append(proc)
            check(client)
        except Exception as exc:  # noqa: BLE001 - script checker reports all cases
            failures += 1
            print(f"FAIL: {check.__name__}: {exc}", file=sys.stderr)
        finally:
            if client is not None:
                try:
                    client.close()
                except OSError:
                    pass
    for proc in procs:
        if proc.poll() is None:
            proc.kill()

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
