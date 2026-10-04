package session_test

import (
	"errors"
	"testing"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

func newLoopSession(t *testing.T) *session.Session {
	t.Helper()
	programs := executor.BuiltinPrograms()
	sess := session.New("bp", executor.NewVirtual(programs["loop"]))
	t.Cleanup(sess.Shutdown)
	return sess
}

func attachController(t *testing.T, sess *session.Session, clientID string) {
	t.Helper()
	result, err := sess.Attach(clientID, true, false)
	if err != nil || result.Role != session.RoleController {
		t.Fatalf("attach %s = %+v, %v", clientID, result, err)
	}
}

func continueCurrent(t *testing.T, sess *session.Session, clientID string) session.ContinueResult {
	t.Helper()
	snap := sess.Snapshot()
	result, err := sess.ContinueEx(clientID, snap.PauseID, "")
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	return result
}

// TestCandidatePreviewObserverCanViewControllerAdopts verifies observers can
// prepare and inspect candidates but only the controller adopts.
func TestCandidatePreviewObserverCanViewControllerAdopts(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.Attach("bob", true, false); err != nil {
		t.Fatal(err)
	}

	view, err := sess.PreviewBreakpoints("bob", []executor.Breakpoint{{Line: 2, Enabled: true, Condition: "i >= 2"}})
	if err != nil {
		t.Fatalf("observer preview: %v", err)
	}
	if view.Stale || view.Candidate.BaseVersion != 0 || !view.Candidate.Items[2].Enabled {
		t.Fatalf("view = %+v", view)
	}
	// The observer sees the same candidate through the read model.
	if got, err := sess.Candidate(); err != nil || got.Candidate.ID != view.Candidate.ID {
		t.Fatalf("candidate view = %+v, %v", got, err)
	}
	// An observer cannot adopt.
	if _, _, err := sess.AdoptCandidate("bob", ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer adopt = %v", err)
	}
	adopted, _, err := sess.AdoptCandidate("alice", "")
	if err != nil {
		t.Fatalf("controller adopt: %v", err)
	}
	if adopted.Version != 1 || adopted.Items[2].Condition != "i >= 2" {
		t.Fatalf("adopted = %+v", adopted)
	}
	if _, err := sess.Candidate(); !errors.Is(err, session.ErrNoCandidate) {
		t.Fatalf("candidate after adopt = %v", err)
	}
}

// TestLateAdoptFromOldControllerRejected covers control handover: after
// alice detaches and bob takes over, alice's late adopt cannot change the
// current version.
func TestLateAdoptFromOldControllerRejected(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	sess.Detach("alice")
	attachController(t, sess, "bob")

	if _, _, err := sess.AdoptCandidate("alice", ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("late adopt = %v", err)
	}
	if snap := sess.Snapshot(); snap.BreakpointVersion != 0 {
		t.Fatalf("version changed by old controller: %+v", snap)
	}
	adopted, _, err := sess.AdoptCandidate("bob", "")
	if err != nil || adopted.Version != 1 {
		t.Fatalf("new controller adopt = %+v, %v", adopted, err)
	}
}

// TestReconnectKeepsVersionAndHits verifies a reconnecting controller gets
// its role back and observes adopted versions and already-occurred hits.
func TestReconnectKeepsVersionAndHits(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sess.AdoptCandidate("alice", ""); err != nil {
		t.Fatal(err)
	}
	continueCurrent(t, sess, "alice") // stops at line 2, hit #1

	result, err := sess.Attach("alice", true, false)
	if err != nil || result.Role != session.RoleController {
		t.Fatalf("reconnect = %+v, %v", result, err)
	}
	if result.Snapshot.BreakpointVersion != 1 || result.Snapshot.Hits[2] != 1 {
		t.Fatalf("snapshot after reconnect = %+v", result.Snapshot)
	}
}

// TestTwoConsecutiveAdoptions checks version increments and that each pause
// records the version actually in effect.
func TestTwoConsecutiveAdoptions(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")

	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true, HitCount: 2}}); err != nil {
		t.Fatal(err)
	}
	first, _, err := sess.AdoptCandidate("alice", "")
	if err != nil || first.Version != 1 {
		t.Fatalf("first adopt = %+v, %v", first, err)
	}
	// Hit-count convention 2: the first reach of line 2 does not stop.
	paused := continueCurrent(t, sess, "alice")
	if paused.Paused == nil || paused.Paused.Line != 2 || paused.Paused.BreakpointVersion != 1 {
		t.Fatalf("pause after v1 = %+v", paused.Paused)
	}
	if paused.Paused.Hits[2] != 2 {
		t.Fatalf("hits = %+v", paused.Paused.Hits)
	}

	// Second adoption prepared on top of v1; the first candidate is gone.
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: false}, {Line: 3, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	second, _, err := sess.AdoptCandidate("alice", "")
	if err != nil || second.Version != 2 {
		t.Fatalf("second adopt = %+v, %v", second, err)
	}
	if second.Items[2].Enabled || !second.Items[3].Enabled {
		t.Fatalf("second items = %+v", second.Items)
	}
	paused = continueCurrent(t, sess, "alice")
	if paused.Paused == nil || paused.Paused.Line != 3 || paused.Paused.BreakpointVersion != 2 {
		t.Fatalf("pause after v2 = %+v", paused.Paused)
	}
	// Hits from version 1 remain visible after the version moved on; the
	// now-disabled line 2 stops accumulating new hits.
	if paused.Paused.Hits[2] != 2 {
		t.Fatalf("hits after v2 = %+v", paused.Paused.Hits)
	}
}

// TestCandidateStaleAfterNewPause ensures a candidate prepared during one
// pause is not silently applied after the program pauses again; the
// re-check info names the locations and the hits that accumulated.
func TestCandidateStaleAfterNewPause(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.SetBreakpointsEx("alice", []int{2}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 3, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	continueCurrent(t, sess, "alice") // new pause at line 2

	view, err := sess.Candidate()
	if err != nil || !view.Stale || view.Recheck == nil {
		t.Fatalf("view = %+v, %v", view, err)
	}
	if len(view.Recheck.Lines) != 2 || view.Recheck.Lines[0] != 2 || view.Recheck.Lines[1] != 3 {
		t.Fatalf("recheck lines = %+v", view.Recheck.Lines)
	}
	if view.Recheck.Hits[2] != 1 {
		t.Fatalf("recheck hits = %+v", view.Recheck.Hits)
	}
	if _, recheck, err := sess.AdoptCandidate("alice", ""); !errors.Is(err, session.ErrStaleCandidate) || recheck == nil {
		t.Fatalf("stale adopt = %v, %+v", err, recheck)
	}
	if snap := sess.Snapshot(); snap.BreakpointVersion != 1 {
		t.Fatalf("stale adopt changed version: %+v", snap)
	}
}

// TestCandidateStaleAfterSourceChange covers source reloads invalidating a
// prepared candidate while keeping already-occurred hits intact.
func TestCandidateStaleAfterSourceChange(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.NoteSourceChange(); err != nil {
		t.Fatal(err)
	}
	_, recheck, err := sess.AdoptCandidate("alice", "")
	if !errors.Is(err, session.ErrStaleCandidate) {
		t.Fatalf("adopt after source change = %v", err)
	}
	if recheck == nil || len(recheck.Lines) != 1 || recheck.Lines[0] != 2 {
		t.Fatalf("recheck = %+v", recheck)
	}
	// Re-preparing on the new source version makes the candidate adoptable.
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	adopted, _, err := sess.AdoptCandidate("alice", "")
	if err != nil || adopted.Version != 1 {
		t.Fatalf("re-prepared adopt = %+v, %v", adopted, err)
	}
}

// TestConditionErrorKeepsDiagnostic verifies a broken condition still stops
// the program and surfaces a diagnostic instead of counting as a miss.
func TestConditionErrorKeepsDiagnostic(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true, Condition: "nosuchvar > 1"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sess.AdoptCandidate("alice", ""); err != nil {
		t.Fatal(err)
	}
	paused := continueCurrent(t, sess, "alice")
	if paused.Paused == nil || paused.Paused.Reason != executor.ReasonBreakpoint {
		t.Fatalf("paused = %+v", paused.Paused)
	}
	if paused.Paused.Diagnostic == "" {
		t.Fatalf("missing diagnostic: %+v", paused.Paused)
	}
	snap := sess.Snapshot()
	if snap.Diagnostic == "" || snap.Hits[2] != 1 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

// TestConditionFiltersHits verifies a passing condition gates the stop.
func TestConditionFiltersHits(t *testing.T) {
	sess := newLoopSession(t)
	attachController(t, sess, "alice")
	if _, err := sess.PreviewBreakpoints("alice", []executor.Breakpoint{{Line: 2, Enabled: true, Condition: "i >= 2"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sess.AdoptCandidate("alice", ""); err != nil {
		t.Fatal(err)
	}
	paused := continueCurrent(t, sess, "alice")
	if paused.Paused == nil || paused.Paused.Vars["i"] < 2 {
		t.Fatalf("paused = %+v", paused.Paused)
	}
}
