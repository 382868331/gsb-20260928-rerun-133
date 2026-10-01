package session_test

import (
	"errors"
	"testing"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

func TestControllerOwnershipAndObserverAccess(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()

	attach, err := sess.Attach("alice", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if attach.Role != session.RoleController || attach.Controller != "alice" {
		t.Fatalf("alice should hold control: %+v", attach)
	}

	second, err := sess.Attach("bob", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if second.Role != session.RoleObserver {
		t.Fatalf("bob must stay observer while alice controls: %+v", second)
	}

	pauseID := sess.Snapshot().PauseID
	if _, err := sess.ContinueEx("bob", pauseID, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer continue error = %v", err)
	}
	if _, err := sess.PauseEx("bob", ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer pause error = %v", err)
	}
	if _, err := sess.SetBreakpointsEx("bob", []int{2}, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer breakpoints error = %v", err)
	}
	if _, err := sess.SetBreakpointsEx("alice", []int{2}, ""); err != nil {
		t.Fatalf("controller breakpoints: %v", err)
	}

	// Observers keep read access to the current pause.
	if _, err := sess.Variables(pauseID, nil); err != nil {
		t.Fatalf("observer variables read: %v", err)
	}

	if _, err := sess.ContinueEx("alice", pauseID, ""); err != nil {
		t.Fatalf("controller continue: %v", err)
	}
}

func TestDetachAndTakeoverHandControlOver(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()

	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	sess.Detach("alice")

	attach, err := sess.Attach("bob", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if attach.Role != session.RoleController {
		t.Fatalf("bob should take control after alice detached: %+v", attach)
	}

	// A still-attached controller is only displaced by an explicit takeover.
	if _, err := sess.Attach("carol", true, false); err != nil {
		t.Fatal(err)
	}
	carol, err := sess.Attach("carol", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if carol.Role != session.RoleController || carol.Controller != "carol" {
		t.Fatalf("takeover should hand control to carol: %+v", carol)
	}
	pauseID := sess.Snapshot().PauseID
	if _, err := sess.ContinueEx("bob", pauseID, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("displaced controller error = %v", err)
	}
}

func TestIdempotentContinueDoesNotAdvanceTwice(t *testing.T) {
	sess := newSession(t, "count")
	defer sess.Close()
	pauseID := sess.Snapshot().PauseID

	first, err := sess.ContinueEx("", pauseID, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Paused == nil && !first.Terminated {
		t.Fatalf("continue should report an outcome: %+v", first)
	}

	again, err := sess.ContinueEx("", pauseID, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate {
		t.Fatalf("retry must be flagged duplicate: %+v", again)
	}
	if (first.Paused == nil) != (again.Paused == nil) || first.Terminated != again.Terminated {
		t.Fatalf("retry must replay the stored outcome: first=%+v again=%+v", first, again)
	}
	if first.Paused != nil && again.Paused != nil && first.Paused.PauseID != again.Paused.PauseID {
		t.Fatalf("retry advanced the program: first=%+v again=%+v", first.Paused, again.Paused)
	}

	// The current pause is exactly the one the first continue produced.
	current := sess.Snapshot()
	if first.Paused != nil && current.PauseID != first.Paused.PauseID {
		t.Fatalf("program advanced beyond the stored outcome: %+v", current)
	}
}

func TestReattachAfterExitKeepsTrustedTermination(t *testing.T) {
	sess := newSession(t, "raise")
	defer sess.Close()

	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	pauseID := sess.Snapshot().PauseID
	result, err := sess.ContinueEx("alice", pauseID, "req-exit")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Terminated || result.Reason != executor.ReasonException {
		t.Fatalf("expected exception termination: %+v", result)
	}

	// A brand-new client attaching after the exit observes the trusted
	// reason and the last pause, and may take control of the ended session.
	attach, err := sess.Attach("bob", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if attach.Snapshot.State != session.StateTerminated {
		t.Fatalf("snapshot after exit: %+v", attach.Snapshot)
	}
	if attach.Snapshot.Reason != executor.ReasonException {
		t.Fatalf("termination reason lost: %+v", attach.Snapshot)
	}
	if attach.Snapshot.LastPause == nil || attach.Snapshot.LastPause.PauseID == 0 {
		t.Fatalf("last pause lost: %+v", attach.Snapshot)
	}

	// The stored outcome is still explained to the retrying controller.
	again, err := sess.ContinueEx("alice", pauseID, "req-exit")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || !again.Terminated || again.Reason != executor.ReasonException {
		t.Fatalf("retry after exit must replay termination: %+v", again)
	}
}
