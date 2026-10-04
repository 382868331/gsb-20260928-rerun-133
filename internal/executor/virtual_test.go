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
