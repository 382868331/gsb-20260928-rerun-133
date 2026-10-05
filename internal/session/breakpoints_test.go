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

	// The preview shows the hit target on the current version, and the
	// recorded hits match the count the pause actually fired on.
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
	if view := sess.BreakpointsView(); view.Current[0].Hits != 6 {
		t.Fatalf("current hits = %+v, want 6 recorded hits", view.Current)
	}
}

func TestHitCountRestartsOnNewVersion(t *testing.T) {
	sess := newSession(t, "loop")
	defer sess.Close()
	if _, err := sess.Attach("alice", true, false); err != nil {
		t.Fatal(err)
	}
	entry := sess.Snapshot()

	first, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 2},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sess.AdoptCandidate("alice", first.ID, ""); err != nil {
		t.Fatal(err)
	}
	paused, err := sess.ContinueEx("alice", entry.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if paused.Paused == nil || paused.Paused.HitCount != 2 {
		t.Fatalf("first pause = %+v, want hit 2", paused)
	}

	// Adopting an identical spec as a new version archives the old hits
	// and restarts the count: the next pause fires on hit 2 of the new
	// version, not on the carried-over hit 4.
	second, err := sess.StageCandidate("alice", []session.BreakpointSpec{
		{Line: 2, Enabled: true, HitCount: 2},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	record, err := sess.AdoptCandidate("alice", second.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := sess.ContinueEx("alice", paused.Paused.PauseID, "")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Paused == nil || resumed.Paused.HitCount != 2 || resumed.Paused.BreakpointVersion != record.ID {
		t.Fatalf("second pause = %+v, want hit 2 under version %d", resumed, record.ID)
	}

	view := sess.BreakpointsView()
	var archived *session.VersionRecord
	for i := range view.History {
		if view.History[i].ID == record.ID-1 {
			archived = &view.History[i]
		}
	}
	if archived == nil || archived.Hits[2] != 2 {
		t.Fatalf("archived hits = %+v, want 2 hits on the replaced version", view.History)
	}
	if len(view.Current) != 1 || view.Current[0].Hits != 2 {
		t.Fatalf("current = %+v, want 2 hits on the new version", view.Current)
	}
}

func TestControlHandoverKeepsHitCounts(t *testing.T) {
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

	// Control moves to bob; the hit records and candidate baselines are
	// session state and survive the handover unchanged.
	sess.Detach("alice")
	if _, err := sess.Attach("bob", true, false); err != nil {
		t.Fatal(err)
	}
	view := sess.BreakpointsView()
	if len(view.Current) != 1 || view.Current[0].Hits != 1 {
		t.Fatalf("hits lost on handover: %+v", view.Current)
	}
	paused := sess.Snapshot()
	if _, err := sess.ContinueEx("bob", paused.PauseID, ""); err != nil {
		t.Fatal(err)
	}
	if view := sess.BreakpointsView(); view.Current[0].Hits != 2 {
		t.Fatalf("hits after handover continue = %+v, want 2", view.Current)
	}
}
