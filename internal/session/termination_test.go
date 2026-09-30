package session_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

// TestTerminationReasonAndLastPause verifies that once the executor is gone
// the session still answers with the termination reason and preserves the
// final pause for query, while a stale pause identity can never read it.
func TestTerminationReasonAndLastPause(t *testing.T) {
	sess := newSession(t, "raise")
	pauseID := sess.Snapshot().PauseID
	if _, err := sess.Continue(pauseID); err != nil {
		t.Fatal(err)
	}

	snap := sess.Snapshot()
	if snap.State != session.StateTerminated || snap.Reason != executor.ReasonException {
		t.Fatalf("terminal snapshot = %+v", snap)
	}
	if snap.LastPause == nil {
		t.Fatal("terminated snapshot must expose the last pause")
	}
	if snap.LastPause.PauseID == 0 || snap.LastPause.State != session.StateStopped {
		t.Fatalf("last pause = %+v", snap.LastPause)
	}
	if wr := sess.Wait(); wr.State != session.StateTerminated {
		t.Fatalf("wait after exit = %+v", wr)
	}

	// The stale identity of the final pause must never read post-termination
	// state through the variables channel.
	if _, err := sess.Variables(pauseID, nil); !errors.Is(err, session.ErrTerminated) {
		t.Fatalf("variables after exit = %v", err)
	}
	if _, err := sess.Continue(pauseID); !errors.Is(err, session.ErrTerminated) {
		t.Fatalf("continue after exit = %v", err)
	}
	sess.Close()
}

// TestStalePauseCannotReadNewState moves to a later pause and ensures the old
// pauseId is rejected for both variables and continue.
func TestStalePauseCannotReadNewState(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	old := sess.Snapshot().PauseID

	if _, err := sess.SetBreakpoints([]int{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Continue(old); err != nil {
		t.Fatal(err)
	}
	current := sess.Snapshot()
	if current.PauseID == old {
		t.Fatal("expected a fresh pause identity")
	}
	if _, err := sess.Variables(old, nil); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("stale variables = %v", err)
	}
	if _, err := sess.Continue(old); !errors.Is(err, session.ErrPauseMismatch) {
		t.Fatalf("stale continue = %v", err)
	}
	if v, err := sess.Variables(current.PauseID, nil); err != nil || v["i"] != 0 {
		t.Fatalf("current pause read = %#v, %v", v, err)
	}
}

// TestConcurrentRequestsAroundExit interleaves continue, read, breakpoint and
// pause requests while the executor terminates abnormally. Every request must
// get a definite answer without a client timeout, and after Close no session
// goroutines may remain.
// TestConcurrentRequestsAroundExit interleaves continue, wait, variable reads,
// breakpoint updates and snapshots until the executor terminates abnormally,
// then keeps issuing requests from the stale identity. Every request must get
// a definite answer; Close must return only after all waits have ended.
func TestConcurrentRequestsAroundExit(t *testing.T) {
	const iterations = 80
	for iter := 0; iter < iterations; iter++ {
		sess := newSession(t, "raise")
		entry := sess.Snapshot().PauseID

		var wg sync.WaitGroup
		errs := make(chan error, 512)
		record := func(err error) { errs <- err }

		// Read-only mixed load that never changes the control flow, so the
		// program still runs to its exception. Variables use the entry id, and
		// once execution reaches a later pause they must observe ErrPauseMismatch
		// rather than reading new state.
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				for step := 0; step < 10; step++ {
					id := entry
					if (seed+step)%2 == 0 {
						id = entry + 7
					}
					switch (seed + step) % 3 {
					case 0:
						_, err := sess.Variables(id, []string{"ready"})
						record(err)
					case 1:
						_, err := sess.SetBreakpoints(nil)
						record(err)
					default:
						snap := sess.Snapshot()
						if snap.State == "" {
							record(errors.New("empty snapshot"))
						} else {
							record(nil)
						}
					}
				}
			}(worker)
		}

		// A waiter that must be released by the abnormal exit.
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := sess.Wait()
			if r.State != session.StateTerminated || r.Reason != executor.ReasonException {
				errs <- errors.New("wait did not observe exception: " + string(r.State))
			}
		}()

		res, err := sess.Continue(entry)
		if err != nil || !res.Terminated || res.Reason != executor.ReasonException {
			t.Fatalf("continue to exit = %+v, %v", res, err)
		}

		// New requests arriving strictly after executor exit.
		for post := 0; post < 24; post++ {
			switch post % 5 {
			case 0:
				r := sess.Wait()
				if r.State != session.StateTerminated {
					t.Fatalf("post-exit wait = %+v", r)
				}
			case 1:
				snap := sess.Snapshot()
				if snap.State != session.StateTerminated || snap.Reason != executor.ReasonException || snap.LastPause == nil {
					t.Fatalf("post-exit snapshot = %+v", snap)
				}
			case 2:
				if _, err := sess.Variables(entry, nil); !errors.Is(err, session.ErrTerminated) {
					t.Fatalf("post-exit variables = %v", err)
				}
			case 3:
				if _, err := sess.SetBreakpoints(nil); !errors.Is(err, session.ErrTerminated) {
					t.Fatalf("post-exit breakpoints = %v", err)
				}
			case 4:
				if _, err := sess.Continue(entry); !errors.Is(err, session.ErrTerminated) {
					t.Fatalf("post-exit continue = %v", err)
				}
			}
		}

		wg.Wait()
		close(errs)
		for got := range errs {
			if got != nil &&
				!errors.Is(got, session.ErrRunning) &&
				!errors.Is(got, session.ErrTerminated) &&
				!errors.Is(got, session.ErrPauseMismatch) &&
				!errors.Is(got, session.ErrInvalidPause) {
				t.Fatalf("unexpected concurrent result: %v", got)
			}
		}

		closed := make(chan struct{})
		go func() {
			sess.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("session resources were not released after all waits ended")
		}

		// Requests after full release still get a definite terminal answer.
		snap := sess.Snapshot()
		if snap.State != session.StateTerminated {
			t.Fatalf("post-close snapshot = %+v", snap)
		}
		if _, err := sess.Variables(entry, nil); !errors.Is(err, session.ErrTerminated) {
			t.Fatalf("post-close variables = %v", err)
		}

		// Heavy concurrent calls after the loop goroutine is gone must all
		// finish with coherent terminal answers instead of parking.
		var postWG sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			postWG.Add(1)
			go func(seed int) {
				defer postWG.Done()
				for step := 0; step < 16; step++ {
					switch (seed + step) % 3 {
					case 0:
						snap := sess.Snapshot()
						if snap.State != session.StateTerminated {
							t.Errorf("post-close snapshot state = %s", snap.State)
						}
					case 1:
						if r := sess.Wait(); r.State != session.StateTerminated {
							t.Errorf("post-close wait = %+v", r)
						}
					default:
						if _, err := sess.Variables(entry, nil); !errors.Is(err, session.ErrTerminated) {
							t.Errorf("post-close vars = %v", err)
						}
					}
				}
			}(worker)
		}
		postDone := make(chan struct{})
		go func() { postWG.Wait(); close(postDone) }()
		select {
		case <-postDone:
		case <-time.After(2 * time.Second):
			t.Fatal("post-close requests stranded without a consumer")
		}
	}
}
