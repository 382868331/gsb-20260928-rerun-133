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
	if !v.UpdateBreakpoints(map[int]bool{2: true}) {
		t.Fatal("update failed")
	}
	v.Resume(entry.PauseID, map[int]bool{2: true})
	next := <-events
	if next.Reason != executor.ReasonBreakpoint || next.Line != 2 {
		t.Fatalf("next = %+v", next)
	}
}
