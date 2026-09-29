package session_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

func newSession(t *testing.T, name string) *session.Session {
	t.Helper()
	programs := executor.BuiltinPrograms()
	return session.New("test", executor.NewVirtual(programs[name]))
}

func TestEntryHandoffAndVariables(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()

	snapshot := sess.Snapshot()
	if snapshot.State != session.StateStopped || snapshot.Reason != executor.ReasonEntry {
		t.Fatalf("unexpected entry snapshot: %+v", snapshot)
	}
	if snapshot.PauseID == 0 {
		t.Fatal("entry pause must have an id")
	}
	vars, err := sess.Variables(snapshot.PauseID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vars["i"] != 0 {
		t.Fatalf("unexpected vars: %#v", vars)
	}
}

func TestDuplicateAndStaleContinue(t *testing.T) {
	sess := newSession(t, "count")
	defer sess.Close()
	firstPause := sess.Snapshot().PauseID

	if _, err := sess.Continue(firstPause + 99); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("stale continue error = %v", err)
	}
	results := make(chan error, 2)
	go func() {
		_, err := sess.Continue(firstPause)
		results <- err
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, err := sess.Continue(firstPause)
		results <- err
	}()
	accepted, rejected := 0, 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, session.ErrRunning) || errors.Is(err, session.ErrPauseMismatch) || errors.Is(err, session.ErrTerminated):
			rejected++
		default:
			t.Fatalf("duplicate continue result = %v", err)
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("continue serialization accepted=%d rejected=%d", accepted, rejected)
	}
}

func TestRunningVariablesRejectedAndNoMixedSnapshot(t *testing.T) {
	sess := newSession(t, "raise")
	defer sess.Close()
	pauseID := sess.Snapshot().PauseID

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := sess.Continue(pauseID)
		errs <- err
	}()
	go func() {
		defer wg.Done()
		_, err := sess.Variables(pauseID, nil)
		errs <- err
	}()
	wg.Wait()
	close(errs)

	successes, rejected := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, session.ErrPauseMismatch), errors.Is(err, session.ErrRunning), errors.Is(err, session.ErrTerminated):
			rejected++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if successes == 0 || successes+rejected != 2 {
		t.Fatalf("concurrent reads returned inconsistent results: success:%d rejected:%d", successes, rejected)
	}
	if successes == 2 && rejected != 0 {
		t.Fatalf("variable read must not mix snapshots, got success:%d rejected:%d", successes, rejected)
	}
}

func TestAbnormalExitReleasesWaiters(t *testing.T) {
	sess := newSession(t, "raise")
	defer sess.Close()
	pauseID := sess.Snapshot().PauseID
	continueDone := make(chan session.ContinueResult, 1)
	continueErr := make(chan error, 1)
	waitDone := make(chan session.WaitResult, 1)

	go func() {
		r, err := sess.Continue(pauseID)
		continueDone <- r
		continueErr <- err
	}()
	go func() {
		waitDone <- sess.Wait()
	}()

	result := <-waitDone
	if result.State != session.StateTerminated || result.Reason != executor.ReasonException {
		t.Fatalf("wait result = %+v", result)
	}
	continued := <-continueDone
	if err := <-continueErr; err != nil || !continued.Terminated || continued.Reason != executor.ReasonException {
		t.Fatalf("continue = %+v, %v", continued, err)
	}
}

func TestBreakpointChangeAppliesAtSafePoint(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	entry := sess.Snapshot().PauseID

	if _, err := sess.SetBreakpoints([]int{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Continue(entry); err != nil {
		t.Fatal(err)
	}
	paused := sess.Snapshot()
	if paused.Reason != executor.ReasonBreakpoint || paused.Line != 2 {
		t.Fatalf("pause location = %+v", paused)
	}
	next, err := sess.Continue(paused.PauseID)
	if err != nil || next.Paused == nil || next.Paused.Line != 2 {
		t.Fatalf("breakpoint result = %+v, %v", next, err)
	}
}
