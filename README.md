# Debug Protocol Service (Go)

A line-delimited JSON debug protocol built around a built-in **virtual
executor**. No GUI and no real process injection are required: the executor
runs a tiny instruction program and pauses at instruction boundaries.

The client-facing request semantics are executor-independent. The first
backend is `virtual`; later backends plug in behind the same commands.

## Build and check

```sh
make build          # builds bin/dbgserver
make test           # go test -race ./...
make check          # builds and runs the local script client
```

Run directly:

```sh
bin/dbgserver -addr 127.0.0.1:8031   # TCP
bin/dbgserver -addr /tmp/dbg.sock    # Unix domain socket
bin/dbgserver -stdio                 # one client over stdin/stdout
```

The script client defaults to spawning `bin/dbgserver -stdio`, so it needs
no listening socket permission:

```sh
python3 scripts/dbgcheck.py
```

## Framing

Each message is one JSON object terminated by `\n`.

Request:

```json
{"id": 1, "command": "continue", "arguments": {"pauseId": 2}}
```

Successful response:

```json
{"type":"response","requestId":1,"command":"continue","success":true,"body":{...}}
```

Error response:

```json
{"type":"response","requestId":1,"command":"continue","success":false,
 "error":{"code":"StalePause","message":"...","currentPauseId":3}}
```

Events are pushed asynchronously:

```json
{"type":"event","event":"stopped","data":{"pauseId":2,"reason":"breakpoint","line":3}}
{"type":"event","event":"continued","data":{"pauseId":2}}
{"type":"event","event":"terminated","data":{"exitCode":9,"message":"kaboom"}}
```

Responses/events for one connection are strictly ordered; in particular the
`launch` response is written before the entry `stopped` event, and the
`continue` response plus `continued` event are written before the next
`stopped`.

## Commands

| Command | Arguments | Purpose |
| --- | --- | --- |
| `initialize` | — | Protocol version, executors and command list |
| `launch` | `executor`, `program`, optional `holdBeforeLine` | Start a session; pauses on entry |
| `setBreakpoints` | `lines` | Atomically replace breakpoints; unknown lines are rejected |
| `continue` | `pauseId`, optional `action` (`continue`/`next`) | Resume exactly one pause generation |
| `variables` | `pauseId` | Atomic variable snapshot for a live pause |
| `stackTrace` | `pauseId` | Current frame/line while paused |
| `status` | — | `paused` / `running` / `exited` with generation and line |
| `releaseHold` | — | Test aid for `holdBeforeLine` |
| `disconnect` | — | Tear the session down |

## Consistency rules

- Every pause gets a fresh monotonically increasing `pauseId`. `continue`,
  `variables` and `stackTrace` carrying an old id fail with `StalePause`
  and include `currentPauseId`; an old pause can never drive a new one.
- `variables` while running fails fast with `NotPaused` instead of blocking
  or returning a mixed snapshot. Reads against a pause see one atomic copy,
  because the engine goroutine is parked while paused.
- `setBreakpoints` validates every line and swaps the whole set at once.
  Breakpoints are evaluated only at instruction boundaries (the safe
  points); an instruction already in flight is never interrupted.
- Duplicate `continue` against a running session fails with `NotPaused`;
  after termination it fails with `ProcessExited`. Nothing is queued.
- Normal or abnormal engine exit closes the exit signal, emits
  `terminated` with the exit code/message, and every blocked waiter and
  later control request fails with `ProcessExited`.

## Virtual program

Instructions use 1-based source lines:

- `{"line":1,"op":"set","var":"x","value":3}`
- `{"line":2,"op":"add","var":"x","a":"x","b":"10"}` (`a`/`b` are variable names or integer literals)
- `{"line":3,"op":"exit","code":0}`
- `{"line":4,"op":"abort","code":9,"message":"kaboom"}`

Omitting `program` uses a built-in arithmetic demo program.
