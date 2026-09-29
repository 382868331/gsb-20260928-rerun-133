package protocol_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"testing"

	"dbgprotocol/internal/protocol"
)

type msg struct {
	Type      string          `json:"type"`
	RequestID int64           `json:"requestId"`
	Success   bool            `json:"success"`
	Body      json.RawMessage `json:"body"`
	Event     string          `json:"event"`
	Data      struct {
		PauseID  int64  `json:"pauseId"`
		Reason   string `json:"reason"`
		Line     int    `json:"line"`
		ExitCode int    `json:"exitCode"`
		Message  string `json:"message"`
	} `json:"data"`
	Err *struct {
		Code           string `json:"code"`
		CurrentPauseID int64  `json:"currentPauseId"`
	} `json:"error"`
}

func dial(t *testing.T) (net.Conn, *bufio.Reader, func()) {
	t.Helper()
	srv := protocol.NewServer()
	clientConn, serverConn := net.Pipe()
	go srv.ServeConn(serverConn)
	cleanup := func() { clientConn.Close(); serverConn.Close() }
	return clientConn, bufio.NewReader(clientConn), cleanup
}

func writeReq(t *testing.T, w io.Writer, id int64, command string, args string) {
	t.Helper()
	if args == "" {
		args = "{}"
	}
	if _, err := io.WriteString(w, `{"id":`+itoa(id)+`,"command":"`+command+`","arguments":`+args+"}\n"); err != nil {
		t.Fatal(err)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func readMsg(t *testing.T, r *bufio.Reader) msg {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m msg
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", line, err)
	}
	return m
}

// End-to-end: launch reply precedes the entry stopped event, and a stale
// continue is rejected over the wire before the real continue proceeds.
func TestWireStaleAndDuplicateContinue(t *testing.T) {
	conn, r, cleanup := dial(t)
	defer cleanup()
	w := bufio.NewWriter(conn)

	writeReq(t, w, 1, "launch", `{"executor":"virtual","holdBeforeLine":2}`)
	w.Flush()
	reply := readMsg(t, r)
	if !reply.Success {
		t.Fatal("launch failed")
	}
	stopped := readMsg(t, r)
	if stopped.Type != "event" || stopped.Event != "stopped" || stopped.Data.PauseID != 1 {
		t.Fatalf("expected entry stopped, got %+v", stopped)
	}

	writeReq(t, w, 2, "setBreakpoints", `{"lines":[3]}`)
	w.Flush()
	if m := readMsg(t, r); !m.Success {
		t.Fatal("setBreakpoints failed")
	}

	// Stale generation first.
	writeReq(t, w, 3, "continue", `{"pauseId":99,"action":"continue"}`)
	w.Flush()
	m := readMsg(t, r)
	if m.Success || m.Err == nil || m.Err.Code != "StalePause" || m.Err.CurrentPauseID != 1 {
		t.Fatalf("stale continue wrong: %+v", m)
	}

	// Correct continue, then duplicate while running.
	writeReq(t, w, 4, "continue", `{"pauseId":1}`)
	w.Flush()
	m = readMsg(t, r)
	if !m.Success {
		t.Fatalf("continue failed: %+v", m)
	}
	m = readMsg(t, r) // continued event
	if m.Event != "continued" {
		t.Fatalf("expected continued event, got %+v", m)
	}
	writeReq(t, w, 5, "continue", `{"pauseId":1}`)
	w.Flush()
	m = readMsg(t, r)
	if m.Success || m.Err == nil || m.Err.Code != "NotPaused" {
		t.Fatalf("duplicate continue should be rejected: %+v", m)
	}

	writeReq(t, w, 8, "releaseHold", `{}`)
	w.Flush()
	if m := readMsg(t, r); !m.Success {
		t.Fatalf("releaseHold failed: %+v", m)
	}
	m = readMsg(t, r) // stopped at breakpoint line 3
	if m.Event != "stopped" || m.Data.PauseID != 2 || m.Data.Line != 3 {
		t.Fatalf("unexpected second stop: %+v", m)
	}

	// Reading variables with the old pause id must fail against the new pause.
	writeReq(t, w, 6, "variables", `{"pauseId":1}`)
	w.Flush()
	m = readMsg(t, r)
	if m.Success || m.Err == nil || m.Err.Code != "StalePause" {
		t.Fatalf("old generation variables should fail: %+v", m)
	}

	writeReq(t, w, 7, "continue", `{"pauseId":2}`)
	w.Flush()
	readMsg(t, r)     // response
	readMsg(t, r)     // continued
	m = readMsg(t, r) // terminated
	if m.Event != "terminated" || m.Data.ExitCode != 0 {
		t.Fatalf("unexpected termination: %+v", m)
	}
}

// Abnormal virtual exit is surfaced with non-zero exit code and message.
func TestWireAbnormalExit(t *testing.T) {
	conn, r, cleanup := dial(t)
	defer cleanup()
	w := bufio.NewWriter(conn)

	program := `[{"line":1,"op":"set","var":"x","value":3},` +
		`{"line":2,"op":"abort","code":9,"message":"kaboom"}]`
	writeReq(t, w, 1, "launch", `{"program":`+program+`}`)
	w.Flush()
	readMsg(t, r) // launch response
	readMsg(t, r) // entry stopped
	writeReq(t, w, 2, "continue", `{"pauseId":1}`)
	w.Flush()
	readMsg(t, r) // continue response
	readMsg(t, r) // continued
	m := readMsg(t, r)
	if m.Event != "terminated" || m.Data.ExitCode != 9 || m.Data.Message != "kaboom" {
		t.Fatalf("bad terminated event: %+v", m)
	}

	// Any later control request fails fast with ProcessExited.
	writeReq(t, w, 3, "variables", `{"pauseId":1}`)
	w.Flush()
	m = readMsg(t, r)
	if m.Success || m.Err == nil || m.Err.Code != "ProcessExited" {
		t.Fatalf("post-exit variables wrong: %+v", m)
	}
}

// Reading variables while running never blocks and never returns mixed data.
func TestWireVariablesWhileRunning(t *testing.T) {
	conn, r, cleanup := dial(t)
	defer cleanup()
	w := bufio.NewWriter(conn)

	program := `[{"line":1,"op":"set","var":"x","value":1},` +
		`{"line":2,"op":"set","var":"x","value":2},` +
		`{"line":3,"op":"exit"}]`
	writeReq(t, w, 1, "launch", `{"program":`+program+
		`,"holdBeforeLine":2}`)
	w.Flush()
	readMsg(t, r)
	readMsg(t, r)
	writeReq(t, w, 2, "continue", `{"pauseId":1}`)
	w.Flush()
	readMsg(t, r)
	readMsg(t, r)

	// The executor is parked before line 2: it is running, not paused.
	writeReq(t, w, 3, "variables", `{"pauseId":1}`)
	w.Flush()
	m := readMsg(t, r)
	if m.Success || m.Err == nil || m.Err.Code != "NotPaused" {
		t.Fatalf("running variables must fail clearly: %+v", m)
	}

	writeReq(t, w, 4, "releaseHold", `{}`)
	w.Flush()
	m = readMsg(t, r)
	if !m.Success {
		t.Fatalf("releaseHold failed: %+v", m)
	}
	m = readMsg(t, r)
	if m.Event != "terminated" {
		t.Fatalf("expected termination after release, got %+v", m)
	}
}
