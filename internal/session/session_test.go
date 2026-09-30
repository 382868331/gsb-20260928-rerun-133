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

func terminalSnapshot(t *testing.T, name string) (*session.Session, int) {
	t.Helper()
	sess := newSession(t, name)
	entry := sess.Snapshot().PauseID
	if _, err := sess.Continue(entry); err != nil {
		t.Fatalf("continue to exit: %v", err)
	}
	if snap := sess.Snapshot(); snap.State != session.StateTerminated {
		t.Fatalf("session not terminated: %+v", snap)
	}
	return sess, entry
}

// Requests already queued before the executor exits and requests created after
// it exits must all receive an explicit result; none may rely on a client
// timeout to bail out of an unconsumed queue.
func TestLateRequestsAfterExitNeverBlock(t *testing.T) {
	sess, entry := terminalSnapshot(t, "raise")
	defer sess.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		const workers = 32
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			i := i
			go func() {
				defer wg.Done()
				switch i % 5 {
				case 0:
					if snap := sess.Snapshot(); snap.State != session.StateTerminated || snap.Reason != executor.ReasonException {
						t.Errorf("snapshot = %+v", snap)
					}
				case 1:
					if w := sess.Wait(); w.State != session.StateTerminated || w.Reason != executor.ReasonException {
						t.Errorf("wait = %+v", w)
					}
				case 2:
					if _, err := sess.Continue(entry); !errors.Is(err, session.ErrTerminated) {
						t.Errorf("late continue err = %v", err)
					}
				case 3:
					if _, err := sess.Variables(entry, nil); !errors.Is(err, session.ErrTerminated) {
						t.Errorf("late variables err = %v", err)
					}
				case 4:
					if _, err := sess.SetBreakpoints([]int{2}); !errors.Is(err, session.ErrTerminated) {
						t.Errorf("late breakpoints err = %v", err)
					}
				}
			}()
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("late requests after executor exit were not all answered")
	}
}

// A terminated snapshot exposes the termination reason and the location of the
// last pause reached before exit (the entry pause for the raise program).
func TestTerminationReasonAndLastPauseQuery(t *testing.T) {
	sess, entry := terminalSnapshot(t, "raise")
	defer sess.Close()

	snap := sess.Snapshot()
	if snap.State != session.StateTerminated || snap.Reason != executor.ReasonException {
		t.Fatalf("terminal snapshot = %+v", snap)
	}
	if snap.LastPauseID != entry || snap.LastLine != 1 || snap.LastPC != 0 {
		t.Fatalf("last pause fields = id:%d line:%d pc:%d, want id:%d line:1 pc:0",
			snap.LastPauseID, snap.LastLine, snap.LastPC, entry)
	}
	waited := sess.Wait()
	if waited.Reason != executor.ReasonException || waited.LastPauseID != entry {
		t.Fatalf("terminal wait = %+v", waited)
	}
}

// An older pause identity must never read or operate the newer pause state.
func TestStalePauseCannotReadNewState(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	entry := sess.Snapshot().PauseID

	if _, err := sess.SetBreakpoints([]int{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Continue(entry); err != nil {
		t.Fatal(err)
	}
	current := sess.Snapshot()
	if current.PauseID == entry {
		t.Fatal("expected a new pause id after continue")
	}
	if _, err := sess.Variables(entry, nil); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("stale variables err = %v", err)
	}
	if _, err := sess.Continue(entry); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("stale continue err = %v", err)
	}
	if vars, err := sess.Variables(current.PauseID, []string{"i"}); err != nil || vars["i"] != 0 {
		t.Fatalf("current pause variables = %#v, %v", vars, err)
	}
}

// Continue, variable reads, breakpoint updates and abnormal exit interleaved
// aggressively: every request gets a definitive result, and only once they
// have all returned does Close release the session resources.
func TestInterruptedRequestsDrainBeforeRelease(t *testing.T) {
	sess := newSession(t, "raise")
	entry := sess.Snapshot().PauseID

	const rounds = 64
	var wg sync.WaitGroup
	wg.Add(rounds*3 + 1)
	release := make(chan struct{})

	for i := 0; i < rounds; i++ {
		go func() {
			defer wg.Done()
			<-release
			_ = sess.Snapshot()
		}()
		go func() {
			defer wg.Done()
			<-release
			_, _ = sess.Variables(entry, []string{"ready"})
		}()
		go func(i int) {
			defer wg.Done()
			<-release
			_, _ = sess.SetBreakpoints([]int{99 + i})
		}(i)
	}
	go func() {
		defer wg.Done()
		<-release
		_, _ = sess.Continue(entry)
	}()

	close(release)
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("interleaved requests did not all settle around abnormal exit")
	}

	closed := make(chan struct{})
	go func() { sess.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("session resources released while waits were outstanding")
	}

	// Even after release, a late request gets an explicit answer rather than
	// parking on a queue with no consumer.
	if snap := sess.Snapshot(); snap.State != session.StateTerminated ||
		snap.Reason != executor.ReasonException {
		t.Fatalf("post-close snapshot = %+v", snap)
	}
}
