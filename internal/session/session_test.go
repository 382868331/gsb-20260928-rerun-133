package session_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"dbgprotocol/internal/session"
	"dbgprotocol/internal/virtual"
)

type recorder struct {
	mu     sync.Mutex
	events []session.Event
}

func collect() (*recorder, func(session.Event)) {
	r := &recorder{}
	return r, func(ev session.Event) {
		r.mu.Lock()
		r.events = append(r.events, ev)
		r.mu.Unlock()
	}
}

func (r *recorder) snapshot() []session.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]session.Event, len(r.events))
	copy(out, r.events)
	return out
}

func waitStatus(t *testing.T, s *session.Session, want string) session.Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := s.State()
		if snap.Status == want {
			return snap
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for status %q, got %v", want, s.State())
	return session.Snapshot{}
}

// A request tied to an old pause generation must never drive the new
// pause: neither resuming it nor reading variables through it is allowed.
func TestStalePauseRejected(t *testing.T) {
	_, emit := collect()
	program := []virtual.Instruction{
		{Line: 1, Op: "set", Var: "x", Value: 1},
		{Line: 2, Op: "set", Var: "x", Value: 2},
		{Line: 3, Op: "exit"},
	}
	engine := virtual.New(program)
	s := session.New("t1", engine, emit)
	s.Start()
	defer s.Close()

	entry := waitStatus(t, s, session.StatusPaused)
	if entry.PauseID != 1 || entry.Line != 1 {
		t.Fatalf("unexpected entry pause: %+v", entry)
	}

	verified, rejected := s.SetBreakpoints([]int{2})
	if len(rejected) != 0 || len(verified) != 1 {
		t.Fatalf("breakpoint verify failed: %v %v", verified, rejected)
	}
	if _, commit, err := s.Resume(entry.PauseID, "continue"); err != nil {
		t.Fatalf("resume: %v", err)
	} else {
		commit()
	}

	second := waitStatus(t, s, session.StatusPaused)
	if second.PauseID != 2 || second.Line != 2 {
		t.Fatalf("unexpected second pause: %+v", second)
	}

	// Duplicate resume against the *old* generation while the new pause is live.
	if _, _, err := s.Resume(entry.PauseID, "continue"); err == nil || err.Code != "StalePause" {
		t.Fatalf("old resume should be StalePause, got %v", err)
	}
	if _, _, err := s.Variables(entry.PauseID); err == nil || err.Code != "StalePause" {
		t.Fatalf("old variable read should be StalePause, got %v", err)
	}

	vars, line, err := s.Variables(second.PauseID)
	if err != nil || vars["x"] != 1 || line != 2 {
		t.Fatalf("current pause variables wrong: %v %d %v", vars, line, err)
	}

	// Duplicate resume against the *current* generation.
	if _, commit, err := s.Resume(second.PauseID, "continue"); err != nil {
		t.Fatalf("resume second: %v", err)
	} else {
		commit()
	}
	waitStatus(t, s, session.StatusExited)
	if _, _, err := s.Resume(second.PauseID, "continue"); err == nil || err.Code != "ProcessExited" {
		t.Fatalf("resume after exit should fail, got %v", err)
	}
}

// Variable inspection while running must be rejected outright so the client
// can never observe a half-mutated mixed snapshot.
func TestVariablesWhileRunningRejected(t *testing.T) {
	_, emit := collect()
	program := []virtual.Instruction{
		{Line: 1, Op: "set", Var: "x", Value: 1},
		{Line: 2, Op: "set", Var: "y", Value: 2},
		{Line: 3, Op: "exit"},
	}
	engine := virtual.New(program)
	releaseHook := make(chan struct{})
	reachedHook := make(chan struct{})
	engine.StepHook = func(line int) {
		if line == 2 {
			close(reachedHook)
			<-releaseHook
		}
	}
	s := session.New("t2", engine, emit)
	s.Start()
	defer s.Close()

	entry := waitStatus(t, s, session.StatusPaused)
	if _, commit, err := s.Resume(entry.PauseID, "continue"); err != nil {
		t.Fatalf("resume: %v", err)
	} else {
		commit()
	}
	<-reachedHook

	done := make(chan *session.Error, 1)
	go func() {
		_, _, err := s.Variables(0)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || err.Code != "NotPaused" {
			t.Fatalf("running read should be NotPaused, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("variables while running blocked instead of failing fast")
	}

	close(releaseHook)
	waitStatus(t, s, session.StatusExited)
}

// Breakpoint edits on invalid lines are rejected; valid ones take effect
// atomically at the next instruction boundary.
func TestBreakpointsSafePoints(t *testing.T) {
	_, emit := collect()
	program := []virtual.Instruction{
		{Line: 10, Op: "set", Var: "x", Value: 1},
		{Line: 20, Op: "set", Var: "x", Value: 2},
		{Line: 30, Op: "exit"},
	}
	engine := virtual.New(program)
	s := session.New("t3", engine, emit)
	s.Start()
	defer s.Close()

	entry := waitStatus(t, s, session.StatusPaused)
	verified, rejected := s.SetBreakpoints([]int{20, 21, 30})
	if len(verified) != 2 || verified[0] != 20 || verified[1] != 30 {
		t.Fatalf("verified lines wrong: %v", verified)
	}
	if len(rejected) != 1 || rejected[0] != 21 {
		t.Fatalf("rejected lines wrong: %v", rejected)
	}
	if _, commit, err := s.Resume(entry.PauseID, "continue"); err != nil {
		t.Fatalf("resume: %v", err)
	} else {
		commit()
	}
	second := waitStatus(t, s, session.StatusPaused)
	if second.Line != 20 {
		t.Fatalf("expected stop on line 20, got %d", second.Line)
	}

	// Atomically clear all breakpoints; line 30 must run through to exit.
	s.SetBreakpoints(nil)
	if _, commit, err := s.Resume(second.PauseID, "continue"); err != nil {
		t.Fatalf("resume: %v", err)
	} else {
		commit()
	}
	waitStatus(t, s, session.StatusExited)
}

// Abnormal executor termination carries the message, unblocks WaitExit and
// every later control request fails with ProcessExited.
func TestAbnormalExitEndsWaiters(t *testing.T) {
	rec, emit := collect()
	program := []virtual.Instruction{
		{Line: 1, Op: "abort", Code: 7, Msg: "boom"},
	}
	engine := virtual.New(program)
	s := session.New("t4", engine, emit)
	s.Start()
	defer s.Close()

	entry := waitStatus(t, s, session.StatusPaused)
	waited := make(chan struct {
		code int
		msg  string
	})
	go func() {
		code, msg, exited := s.WaitExit()
		if !exited {
			code = -1
		}
		waited <- struct {
			code int
			msg  string
		}{code, msg}
	}()
	if _, commit, err := s.Resume(entry.PauseID, "continue"); err != nil {
		t.Fatalf("resume: %v", err)
	} else {
		commit()
	}

	select {
	case r := <-waited:
		if r.code != 7 || r.msg != "boom" {
			t.Fatalf("WaitExit result wrong: %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitExit did not return when executor exited")
	}
	for _, ev := range rec.snapshot() {
		if ev.Type == "terminated" && ev.ExitCode == 7 && ev.Message == "boom" {
			return
		}
	}
	t.Fatalf("missing terminated event: %+v", rec.snapshot())
}

// Concurrent variable reads against the same pause all observe one
// consistent snapshot and never a mixed one.
func TestConcurrentReadsConsistent(t *testing.T) {
	_, emit := collect()
	program := []virtual.Instruction{
		{Line: 1, Op: "set", Var: "x", Value: 11},
		{Line: 2, Op: "set", Var: "y", Value: 22},
		{Line: 3, Op: "exit"},
	}
	engine := virtual.New(program)
	s := session.New("t5", engine, emit)
	s.Start()
	defer s.Close()
	entry := waitStatus(t, s, session.StatusPaused)
	s.SetBreakpoints([]int{2})
	if _, commit, err := s.Resume(entry.PauseID, "continue"); err != nil {
		t.Fatalf("resume: %v", err)
	} else {
		commit()
	}
	second := waitStatus(t, s, session.StatusPaused)

	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			vars, _, err := s.Variables(second.PauseID)
			if err != nil {
				errs <- err
				return
			}
			if len(vars) != 1 || vars["x"] != 11 {
				errs <- fmt.Errorf("mixed/inconsistent snapshot: %v", vars)
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < 16; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent read: %v", err)
		}
	}
}
