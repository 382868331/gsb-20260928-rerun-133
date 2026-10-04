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
	set := executor.BreakpointSet{Version: 1, Items: map[int]executor.Breakpoint{
		2: {Line: 2, Enabled: true},
	}}
	if !v.UpdateBreakpoints(set) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, set)
	next := <-events
	if next.Reason != executor.ReasonBreakpoint || next.Line != 2 {
		t.Fatalf("next = %+v", next)
	}
	if next.BreakpointVersion != 1 || next.Hits[2] != 1 {
		t.Fatalf("version/hits = %+v", next)
	}
}
