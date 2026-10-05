package executor_test

import (
	"testing"

	"debugservice/internal/executor"
)

func TestBreakpointResume(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["loop"])
	events := make(chan executor.Event, 4)
	v.Start(events)
	entry := <-events
	if entry.Reason != executor.ReasonEntry {
		t.Fatalf("entry = %+v", entry)
	}
	if !v.UpdateBreakpoints(map[int]executor.Breakpoint{2: {Line: 2, Enabled: true}}, false) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, map[int]executor.Breakpoint{2: {Line: 2, Enabled: true}})
	next := <-events
	if next.Reason != executor.ReasonBreakpoint || next.Line != 2 {
		t.Fatalf("next = %+v", next)
	}
}

func TestConditionErrorStopsWithDiagnostic(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["count"])
	events := make(chan executor.Event, 4)
	v.Start(events)
	entry := <-events
	bps := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, Condition: "missing > 1"}}
	if !v.UpdateBreakpoints(bps, false) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	next := <-events
	if next.Reason != executor.ReasonConditionError {
		t.Fatalf("reason = %q, want condition-error", next.Reason)
	}
	if next.ConditionError == "" {
		t.Fatal("diagnostic missing on condition error stop")
	}
}

func TestHitCountStopsOnNthHit(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["loop"])
	events := make(chan executor.Event, 4)
	v.Start(events)
	entry := <-events
	bps := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, HitCount: 3}}
	if !v.UpdateBreakpoints(bps, false) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	first := <-events
	if first.Reason != executor.ReasonBreakpoint || first.HitCount != 3 {
		t.Fatalf("first stop = %+v, want breakpoint at hit 3", first)
	}
	v.Resume(first.PauseID, bps)
	second := <-events
	if second.Reason != executor.ReasonBreakpoint || second.HitCount != 6 {
		t.Fatalf("second stop = %+v, want breakpoint at hit 6", second)
	}
}

func TestConditionErrorStopsBeforeHitCountReached(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["loop"])
	events := make(chan executor.Event, 4)
	v.Start(events)
	entry := <-events
	bps := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, Condition: "missing > 1", HitCount: 100}}
	if !v.UpdateBreakpoints(bps, false) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	next := <-events
	if next.Reason != executor.ReasonConditionError || next.ConditionError == "" {
		t.Fatalf("stop = %+v, want condition-error with diagnostic", next)
	}
}

func TestHitCountsResetOnSpecChange(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["loop"])
	events := make(chan executor.Event, 4)
	v.Start(events)
	entry := <-events
	bps := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, HitCount: 2}}
	if !v.UpdateBreakpoints(bps, false) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	first := <-events
	if first.HitCount != 2 {
		t.Fatalf("first stop = %+v, want hit 2", first)
	}
	// Reinstalling the identical spec keeps the accumulated count.
	if !v.UpdateBreakpoints(bps, false) {
		t.Fatal("update failed")
	}
	v.Resume(first.PauseID, bps)
	second := <-events
	if second.HitCount != 4 {
		t.Fatalf("second stop = %+v, want hit 4 carried over", second)
	}
	// Changing the condition restarts the count from zero.
	changed := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, Condition: "i > 0", HitCount: 2}}
	if !v.UpdateBreakpoints(changed, false) {
		t.Fatal("update failed")
	}
	v.Resume(second.PauseID, changed)
	third := <-events
	if third.HitCount != 2 {
		t.Fatalf("third stop = %+v, want hit 2 after spec change", third)
	}
	// A new version reinstalls even an identical spec with resetHits:
	// the count restarts from zero.
	if !v.UpdateBreakpoints(changed, true) {
		t.Fatal("update failed")
	}
	v.Resume(third.PauseID, changed)
	fourth := <-events
	if fourth.HitCount != 2 {
		t.Fatalf("fourth stop = %+v, want hit 2 after version reset", fourth)
	}
}
