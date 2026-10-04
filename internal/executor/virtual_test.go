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
	if !v.UpdateBreakpoints(map[int]executor.Breakpoint{2: {Line: 2, Enabled: true}}) {
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
	if !v.UpdateBreakpoints(bps) {
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
	if !v.UpdateBreakpoints(bps) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	next := <-events
	if next.Reason != executor.ReasonBreakpoint || next.Line != 2 {
		t.Fatalf("next = %+v", next)
	}
	if next.Hits != 3 || next.HitTarget != 3 {
		t.Fatalf("hits = %d target = %d, want 3/3", next.Hits, next.HitTarget)
	}
	if next.Vars["i"] != 2 {
		t.Fatalf("stopped at i = %d, want 2 (third passing hit)", next.Vars["i"])
	}
}

func TestHitCountDoesNotMaskConditionError(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["loop"])
	events := make(chan executor.Event, 4)
	v.Start(events)
	entry := <-events
	bps := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, Condition: "missing > 1", HitCount: 100}}
	if !v.UpdateBreakpoints(bps) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	next := <-events
	if next.Reason != executor.ReasonConditionError {
		t.Fatalf("reason = %q, want condition-error despite unmet hit count", next.Reason)
	}
	if next.ConditionError == "" {
		t.Fatal("diagnostic missing on condition error stop")
	}
}

func TestHitCountsResetOnUpdate(t *testing.T) {
	programs := executor.BuiltinPrograms()
	v := executor.NewVirtual(programs["loop"])
	events := make(chan executor.Event, 8)
	v.Start(events)
	entry := <-events
	bps := map[int]executor.Breakpoint{2: {Line: 2, Enabled: true, HitCount: 2}}
	if !v.UpdateBreakpoints(bps) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, bps)
	first := <-events
	if first.Hits != 2 {
		t.Fatalf("first stop hits = %d, want 2", first.Hits)
	}
	// Reinstalling the set re-bases the counters: the next stop is again
	// the 2nd hit of the new set, not the 4th overall.
	if !v.UpdateBreakpoints(bps) {
		t.Fatal("update failed")
	}
	v.Resume(first.PauseID, bps)
	second := <-events
	if second.Reason != executor.ReasonBreakpoint || second.Hits != 2 {
		t.Fatalf("second stop = %+v, want breakpoint with hits 2", second)
	}
}
