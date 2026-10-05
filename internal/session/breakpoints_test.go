package session_test

import (
	"errors"
	"testing"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

func findCandidate(view session.BreakpointsView, id int) *session.Candidate {
	for i := range view.Candidates {
		if view.Candidates[i].ID == id {
			return &view.Candidates[i]
		}
	}
	return nil
}

func TestCandidateAdoptionEffectiveFromNextContinue(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot()

	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true},
		{Line: 3, Enabled: false},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.State != session.CandidatePending || candidate.BasePauseID != entry.PauseID {
		t.Fatalf("candidate = %+v", candidate)
	}

	// Observers can preview the candidate and the versions.
	view := sess.BreakpointsView()
	if findCandidate(view, candidate.ID) == nil {
		t.Fatalf("candidate missing from preview: %+v", view)
	}
	if view.Version != 1 {
		t.Fatalf("version before adoption = %d", view.Version)
	}

	record, err := sess.AdoptCandidate("alice", candidate.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if record.ID != 2 || record.EffectivePauseID != entry.PauseID {
		t.Fatalf("adopted record = %+v", record)
	}

	// The entry pause still reports the version it ran under; the adopted
	// version takes effect from the next continue.
	if entry.BreakpointVersion != 1 {
		t.Fatalf("entry pause version = %d", entry.BreakpointVersion)
	}
	result, err := sess.ContinueEx("alice", entry.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Paused == nil || result.Paused.Line != 2 {
		t.Fatalf("continue = %+v", result)
	}
	if result.Paused.BreakpointVersion != 2 {
		t.Fatalf("pause ran under version %d, want 2", result.Paused.BreakpointVersion)
	}
	if sess.Snapshot().BreakpointVersion != 2 {
		t.Fatalf("snapshot version = %+v", sess.Snapshot())
	}
}

func TestOnlyCurrentControllerCanAdopt(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Attach("bob", true, false); err != nil {
		t.Fatal(err)
	}
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 2, Enabled: true}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("bob", candidate.ID, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer adopt error = %v", err)
	}

	// Control moves to bob; alice's late adopt must not change the version.
	if _, err := sess.Attach("bob", true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("displaced controller adopt error = %v", err)
	}
	if view := sess.BreakpointsView(); view.Version != 1 {
		t.Fatalf("late adopt changed version to %d", view.Version)
	}
	if _, err := sess.AdoptCandidate("bob", candidate.ID, ""); err != nil {
		t.Fatalf("new controller adopt: %v", err)
	}
	if view := sess.BreakpointsView(); view.Version != 2 {
		t.Fatalf("version after adopt = %d", view.Version)
	}
}

func TestNewPauseFlagsCandidateHits(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.SetBreakpointsEx("alice", []int{2}, ""); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot().PauseID
	if _, err := sess.ContinueEx("alice", entry, ""); err != nil {
		t.Fatal(err)
	}
	// Paused at line 2; one hit recorded for the current version.
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 2, Enabled: true}, {Line: 3, Enabled: true}}, "")
	if err != nil {
		t.Fatal(err)
	}
	paused := sess.Snapshot()
	if _, err := sess.ContinueEx("alice", paused.PauseID, ""); err != nil {
		t.Fatal(err)
	}

	// The hit on line 2 since staging flags the candidate; adopting it
	// silently is refused.
	view := sess.BreakpointsView()
	staged := findCandidate(view, candidate.ID)
	if staged == nil || staged.Recheck == nil || staged.Recheck.Reason != session.RecheckNewPause {
		t.Fatalf("candidate recheck = %+v", staged)
	}
	if len(staged.Recheck.HitLines) != 1 || staged.Recheck.HitLines[0] != 2 {
		t.Fatalf("hit lines = %+v", staged.Recheck)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); !errors.Is(err, session.ErrCandidateStale) {
		t.Fatalf("stale adopt error = %v", err)
	}

	refreshed, err := sess.RefreshCandidate("alice", candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Recheck != nil {
		t.Fatalf("refresh did not clear recheck: %+v", refreshed)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); err != nil {
		t.Fatalf("adopt after refresh: %v", err)
	}
}

func TestSourceChangeFlagsCandidateLocations(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 2, Enabled: true}, {Line: 9, Enabled: true}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.NotifySourceChange("alice", []int{2}); err != nil {
		t.Fatal(err)
	}
	view := sess.BreakpointsView()
	staged := findCandidate(view, candidate.ID)
	if staged == nil || staged.Recheck == nil || staged.Recheck.Reason != session.RecheckSourceChanged {
		t.Fatalf("recheck = %+v", staged)
	}
	if len(staged.Recheck.Lines) != 1 || staged.Recheck.Lines[0] != 9 {
		t.Fatalf("stale lines = %+v", staged.Recheck)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); !errors.Is(err, session.ErrCandidateStale) {
		t.Fatalf("adopt after source change = %v", err)
	}
	if err := sess.NotifySourceChange("bob", nil); !errors.Is(err, session.ErrNotController) {
		t.Fatalf("observer source change = %v", err)
	}
}

func TestConditionErrorKeepsDiagnostic(t *testing.T) {
	sess := newSession(t, "count")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, Condition: "nope > 1"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot().PauseID
	result, err := sess.ContinueEx("alice", entry, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Paused == nil || result.Paused.Reason != executor.ReasonConditionError {
		t.Fatalf("continue = %+v", result)
	}
	if result.Paused.ConditionError == "" {
		t.Fatalf("diagnostic lost: %+v", result.Paused)
	}
	view := sess.BreakpointsView()
	if len(view.Current) != 1 || view.Current[0].ConditionError == "" {
		t.Fatalf("status keeps diagnostic: %+v", view.Current)
	}
	if view.Current[0].Hits != 0 {
		t.Fatalf("condition error must not count as hit: %+v", view.Current[0])
	}
}

func TestTwoConsecutiveAdoptionsAndHitAccounting(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot().PauseID

	// First adoption: break on line 2.
	first, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 2, Enabled: true}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", first.ID, ""); err != nil {
		t.Fatal(err)
	}
	paused, err := sess.ContinueEx("alice", entry, "")
	if err != nil {
		t.Fatal(err)
	}
	if paused.Paused == nil || paused.Paused.BreakpointVersion != 2 {
		t.Fatalf("first pause = %+v", paused)
	}

	// Second adoption while paused: break on line 3 instead. The line-2
	// hit belongs to version 2 and must stay archived.
	second, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 3, Enabled: true}}, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err := sess.AdoptCandidate("alice", second.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if record.ID != 3 || record.EffectivePauseID != paused.Paused.PauseID {
		t.Fatalf("second record = %+v", record)
	}
	resumed, err := sess.ContinueEx("alice", paused.Paused.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Paused == nil || resumed.Paused.Line != 3 || resumed.Paused.BreakpointVersion != 3 {
		t.Fatalf("second pause = %+v", resumed)
	}

	view := sess.BreakpointsView()
	if view.Version != 3 || view.PauseVersion != 3 {
		t.Fatalf("view versions = %+v", view)
	}
	// Version 2 archived with its line-2 hit; version 3 owns the line-3 hit.
	var archived *session.VersionRecord
	for i := range view.History {
		if view.History[i].ID == 2 {
			archived = &view.History[i]
		}
	}
	if archived == nil || archived.Hits[2] != 1 {
		t.Fatalf("archived hits = %+v", view.History)
	}
	if len(view.Current) != 1 || view.Current[0].Line != 3 || view.Current[0].Hits != 1 {
		t.Fatalf("current status = %+v", view.Current)
	}
	// The adopted candidates are no longer pending.
	for _, c := range view.Candidates {
		if c.State == session.CandidatePending {
			t.Fatalf("candidate still pending: %+v", c)
		}
	}
}

func TestReconnectKeepsVersionsAndCandidates(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 2, Enabled: true}}, "req-stage")
	if err != nil {
		t.Fatal(err)
	}
	// Reconnect: same client id, idempotent replay of the staging request.
	attach, err := sess.Attach("alice", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if attach.Role != session.RoleController {
		t.Fatalf("reconnect role = %+v", attach)
	}
	again, err := sess.StageCandidate("alice", []session.BreakpointSpec{{Line: 2, Enabled: true}}, "req-stage")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != candidate.ID {
		t.Fatalf("replayed staging created candidate %d, want %d", again.ID, candidate.ID)
	}
	if view := sess.BreakpointsView(); len(view.Candidates) != 1 {
		t.Fatalf("duplicate candidate staged: %+v", view.Candidates)
	}
}

func TestHitCountCandidatePauseBasis(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot()

	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 3},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err := sess.AdoptCandidate("alice", candidate.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Breakpoints) != 1 || record.Breakpoints[0].HitCount != 3 {
		t.Fatalf("adopted record = %+v, want hitCount 3", record)
	}

	result, err := sess.ContinueEx("alice", entry.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Paused == nil || result.Paused.Reason != "breakpoint" {
		t.Fatalf("continue = %+v, want breakpoint pause", result)
	}
	// The pause records both the version and the hit count it fired on.
	if result.Paused.HitCount != 3 || result.Paused.BreakpointVersion != record.ID {
		t.Fatalf("pause basis = version %d hit %d, want version %d hit 3",
			result.Paused.BreakpointVersion, result.Paused.HitCount, record.ID)
	}
	if snap := sess.Snapshot(); snap.HitCount != 3 {
		t.Fatalf("snapshot hit count = %d, want 3", snap.HitCount)
	}

	// The preview shows the hit target on the current version and the
	// raw number of condition passes, identical to the pause basis, so
	// the current view and the pause record explain the same stop.
	view := sess.BreakpointsView()
	if len(view.Current) != 1 || view.Current[0].HitCount != 3 || view.Current[0].Hits != 3 {
		t.Fatalf("current = %+v, want hitCount 3 with 3 recorded hits", view.Current)
	}

	// A second continue fires again on the next multiple of the target.
	again, err := sess.ContinueEx("alice", result.Paused.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if again.Paused == nil || again.Paused.HitCount != 6 {
		t.Fatalf("second pause = %+v, want hit 6", again)
	}
}

// TestFourthHitPauseBooksConsistently is the reported regression: with a
// hitCount:4 breakpoint the fourth condition pass pauses, and the current
// status and archived history must each show four condition passes for that
// breakpoint instead of collapsing the stop to a single unexplained hit.
func TestFourthHitPauseBooksConsistently(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot()
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 4},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err := sess.AdoptCandidate("alice", candidate.ID, "")
	if err != nil {
		t.Fatal(err)
	}

	result, err := sess.ContinueEx("alice", entry.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Paused == nil || result.Paused.HitCount != 4 {
		t.Fatalf("pause = %+v, want stop on the 4th condition pass", result.Paused)
	}
	if snap := sess.Snapshot(); snap.HitCount != 4 {
		t.Fatalf("snapshot hit count = %d, want 4", snap.HitCount)
	}
	view := sess.BreakpointsView()
	if len(view.Current) != 1 || view.Current[0].Hits != 4 {
		t.Fatalf("current hits = %+v, want 4 condition passes", view.Current)
	}

	// A candidate staged at this pause records the same 4-pass basis.
	staged, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 4},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	refreshed := findCandidate(sess.BreakpointsView(), staged.ID)
	if refreshed == nil || refreshed.Recheck != nil {
		t.Fatalf("fresh candidate recheck = %+v", refreshed)
	}

	// Adopting the unchanged version archives four condition passes with
	// the version that produced the pause; both books agree.
	next, err := sess.AdoptCandidate("alice", staged.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	view = sess.BreakpointsView()
	var archived *session.VersionRecord
	for i := range view.History {
		if view.History[i].ID == record.ID {
			archived = &view.History[i]
		}
	}
	if archived == nil || archived.Hits[2] != 4 {
		t.Fatalf("version %d archived hits = %+v, want 4", record.ID, view.History)
	}
	if next.ID != record.ID+1 {
		t.Fatalf("new version id = %d", next.ID)
	}
	if len(view.Current) != 1 || view.Current[0].Hits != 0 {
		t.Fatalf("new version must start from zero: %+v", view.Current)
	}
}

// TestSilentPassesReconciledOnManualPause proves passes short of the target
// (which never emit a stop) still show up in the books once another stop
// lands, so a later hitCount pause can be explained.
func TestSilentPassesReconciledOnManualPause(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 100},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot().PauseID
	// Resume and immediately request a boundary stop; some passes on the
	// huge target happen before the request lands, but the target cannot
	// be reached. The raw passes must be reconciled into the current view.
	continueErr := make(chan error, 1)
	go func() {
		_, err := sess.ContinueEx("alice", entry, "")
		continueErr <- err
	}()
	paused, err := sess.PauseEx("alice", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := <-continueErr; err != nil {
		t.Fatal(err)
	}
	if paused.State != session.StateStopped {
		t.Fatalf("pause result = %+v", paused)
	}
	if paused.Reason == executor.ReasonBreakpoint {
		t.Fatalf("target unexpectedly reached: %+v", paused)
	}
	view := sess.BreakpointsView()
	if len(view.Current) != 1 || view.Current[0].Hits == 0 {
		t.Fatalf("silent passes not reconciled: %+v", view.Current)
	}
	if paused.HitCount != 0 {
		t.Fatalf("manual pause must not claim a hit basis: %+v", paused)
	}
}

// TestConditionErrorReconcilesSiblingHits verifies a condition error stops
// immediately with its diagnostic and is never a hit, while passes on other
// breakpoints that advanced before the error stop are still accounted.
func TestConditionErrorReconcilesSiblingHits(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	// Line 2 passes on a huge target (silently); line 3 breaks its
	// condition. Line 2 executes first in the loop, so at least one silent
	// pass must already be on the books when the line-3 diagnostic fires.
	candidate, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 100},
		{Line: 3, Enabled: true, Condition: "missing > 1"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", candidate.ID, ""); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot().PauseID
	result, err := sess.ContinueEx("alice", entry, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Paused == nil || result.Paused.Reason != executor.ReasonConditionError {
		t.Fatalf("continue = %+v, want immediate condition-error", result.Paused)
	}
	if result.Paused.ConditionError == "" {
		t.Fatalf("diagnostic missing: %+v", result.Paused)
	}
	view := sess.BreakpointsView()
	statuses := map[int]session.BreakpointStatus{}
	for _, status := range view.Current {
		statuses[status.Line] = status
	}
	if statuses[3].Hits != 0 || statuses[3].ConditionError == "" {
		t.Fatalf("errored breakpoint = %+v, want zero hits with diagnostic", statuses[3])
	}
	if statuses[2].Hits < 1 {
		t.Fatalf("sibling breakpoint passes lost: %+v", statuses[2])
	}
}

// TestSpecChangeResetsCounterAndControlHandoffKeepsIt documents which
// counts survive: changing the condition resets the line to zero, while a
// controller handoff leaves versions and counters untouched.
func TestSpecChangeResetsCounterAndControlHandoffKeepsIt(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot().PauseID
	first, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", first.ID, ""); err != nil {
		t.Fatal(err)
	}
	stopped, err := sess.ContinueEx("alice", entry, "")
	if err != nil {
		t.Fatal(err)
	}
	if view := sess.BreakpointsView(); view.Current[0].Hits != 1 {
		t.Fatalf("hits before change = %+v", view.Current)
	}

	// Change the condition: same line, new spec, counter restarts at zero.
	changed, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, Condition: "i >= 0"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", changed.ID, ""); err != nil {
		t.Fatal(err)
	}
	resumed, err := sess.ContinueEx("alice", stopped.Paused.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Paused == nil {
		t.Fatalf("continue = %+v", resumed)
	}
	if view := sess.BreakpointsView(); view.Current[0].Hits != 1 {
		t.Fatalf("new condition must count from zero: %+v", view.Current)
	}

	// Control handoff keeps the exact books; the new controller sees the
	// same version, counters, and pause basis.
	sess.Detach("alice")
	if _, err := sess.Attach("bob", true, false); err != nil {
		t.Fatal(err)
	}
	view := sess.BreakpointsView()
	if view.Current[0].Hits != 1 {
		t.Fatalf("handoff altered hits: %+v", view.Current)
	}
	if snap := sess.Snapshot(); snap.HitCount != resumed.Paused.HitCount {
		t.Fatalf("handoff altered pause basis: %+v", snap)
	}
}
