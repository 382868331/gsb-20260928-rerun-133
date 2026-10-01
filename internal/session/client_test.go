package session_test

import (
	"errors"
	"testing"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

func TestAttachRolesAndReconnect(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()

	first := sess.Attach("alice")
	if first.Role != session.RoleController || first.Token == "" {
		t.Fatalf("first attach = %+v", first)
	}
	second := sess.Attach("bob")
	if second.Role != session.RoleObserver || second.Token == first.Token {
		t.Fatalf("second attach = %+v", second)
	}
	// Reconnect: same client id regains the same token and role.
	again := sess.Attach("alice")
	if again.Role != session.RoleController || again.Token != first.Token {
		t.Fatalf("reconnect = %+v", again)
	}
	if again.Snapshot.State != session.StateStopped || again.Snapshot.PauseID == 0 {
		t.Fatalf("reconnect snapshot = %+v", again.Snapshot)
	}
	if again.Snapshot.Controller != "alice" {
		t.Fatalf("snapshot controller = %q", again.Snapshot.Controller)
	}
}

func TestObserverCannotAdvanceButCanQuery(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	controller := sess.Attach("alice")
	observer := sess.Attach("bob")
	pauseID := sess.Snapshot().PauseID

	if _, err := sess.ContinueAs(observer.Token, pauseID, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer continue = %v", err)
	}
	if _, err := sess.PauseAs(observer.Token, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer pause = %v", err)
	}
	if _, err := sess.SetBreakpointsAs(observer.Token, []int{2}); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer breakpoints = %v", err)
	}
	// Read-only queries stay open to observers.
	if _, err := sess.Variables(pauseID, nil); err != nil {
		t.Fatalf("observer variables = %v", err)
	}
	if _, err := sess.SetBreakpointsAs(controller.Token, []int{2}); err != nil {
		t.Fatalf("controller breakpoints = %v", err)
	}
	if _, err := sess.ContinueAs(controller.Token, pauseID, ""); err != nil {
		t.Fatalf("controller continue = %v", err)
	}
}

func TestReleaseAndClaimControl(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	alice := sess.Attach("alice")
	bob := sess.Attach("bob")

	if err := sess.Claim(bob.Token); !errors.Is(err, session.ErrControlHeld) {
		t.Fatalf("claim while held = %v", err)
	}
	if err := sess.Release(bob.Token); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("release by observer = %v", err)
	}
	if err := sess.Release(alice.Token); err != nil {
		t.Fatalf("release = %v", err)
	}
	pauseID := sess.Snapshot().PauseID
	if _, err := sess.ContinueAs(alice.Token, pauseID, ""); !errors.Is(err, session.ErrNoController) {
		t.Fatalf("continue with vacant control = %v", err)
	}
	if err := sess.Claim(bob.Token); err != nil {
		t.Fatalf("claim = %v", err)
	}
	if _, err := sess.SetBreakpointsAs(bob.Token, []int{2}); err != nil {
		t.Fatalf("breakpoints after takeover = %v", err)
	}
	if _, err := sess.ContinueAs(bob.Token, pauseID, ""); err != nil {
		t.Fatalf("continue after takeover = %v", err)
	}
	if got := sess.Snapshot().Controller; got != "bob" {
		t.Fatalf("controller after takeover = %q", got)
	}
}

func TestIdempotentContinueReplay(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	client := sess.Attach("alice")
	pauseID := sess.Snapshot().PauseID
	if _, err := sess.SetBreakpointsAs(client.Token, []int{2}); err != nil {
		t.Fatal(err)
	}

	first, err := sess.ContinueAs(client.Token, pauseID, "req-1")
	if err != nil || first.Duplicate {
		t.Fatalf("first continue = %+v, %v", first, err)
	}
	replay, err := sess.ContinueAs(client.Token, pauseID, "req-1")
	if err != nil {
		t.Fatalf("replay continue = %v", err)
	}
	if !replay.Duplicate {
		t.Fatalf("replay must be marked duplicate: %+v", replay)
	}
	// The replay must not have advanced the program: state is identical to
	// what the first request observed.
	if (first.Paused == nil) != (replay.Paused == nil) || first.Terminated != replay.Terminated {
		t.Fatalf("replay outcome mismatch: first=%+v replay=%+v", first, replay)
	}
	if first.Paused != nil && replay.Paused.PauseID != first.Paused.PauseID {
		t.Fatalf("replay pause = %+v, want %+v", replay.Paused, first.Paused)
	}
	// A replayed rejection is explained as duplicate too.
	if _, err := sess.ContinueAs(client.Token, 9999, "req-2"); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("stale continue = %v", err)
	}
	if _, err := sess.ContinueAs(client.Token, 9999, "req-2"); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("replayed stale continue = %v", err)
	}
}

func TestReconnectAfterTerminationKeepsReasonAndLastPause(t *testing.T) {
	sess := newSession(t, "raise")
	defer sess.Close()
	controller := sess.Attach("alice")
	pauseID := sess.Snapshot().PauseID

	result, err := sess.ContinueAs(controller.Token, pauseID, "req-exit")
	if err != nil || !result.Terminated || result.Reason != executor.ReasonException {
		t.Fatalf("continue to exit = %+v, %v", result, err)
	}
	// Controller disconnects; a brand-new client attaches after the exit and
	// must observe the trusted termination reason and last pause.
	late := sess.Attach("carol")
	if late.Snapshot.State != session.StateTerminated || late.Snapshot.Reason != executor.ReasonException {
		t.Fatalf("late attach snapshot = %+v", late.Snapshot)
	}
	if late.Snapshot.LastPause == nil || late.Snapshot.LastPause.PauseID != pauseID {
		t.Fatalf("late attach lastPause = %+v", late.Snapshot.LastPause)
	}
	// The recorded exit outcome is replayed, not re-executed.
	replay, err := sess.ContinueAs(controller.Token, pauseID, "req-exit")
	if err != nil {
		t.Fatalf("replayed exit continue err = %v", err)
	}
	if !replay.Duplicate || !replay.Terminated || replay.Reason != executor.ReasonException {
		t.Fatalf("replayed exit continue = %+v", replay)
	}
}
