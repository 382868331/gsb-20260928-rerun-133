package executor

import (
	"sync"
	"sync/atomic"
	"time"
)

const StepInterval = 5 * time.Millisecond

type Op string

const (
	OpSet   Op = "set"
	OpAdd   Op = "add"
	OpJump  Op = "jump"
	OpRaise Op = "raise"
)

type Instruction struct {
	Op    Op     `json:"op"`
	Line  int    `json:"line"`
	Name  string `json:"name,omitempty"`
	Value int64  `json:"value,omitempty"`
}

type Program struct {
	Name string        `json:"name"`
	Code []Instruction `json:"code"`
}

func BuiltinPrograms() map[string]Program {
	return map[string]Program{
		"count": {
			Name: "count",
			Code: []Instruction{
				{Op: OpSet, Line: 1, Name: "i", Value: 1},
				{Op: OpAdd, Line: 2, Name: "i", Value: 1},
			},
		},
		"loop": {
			Name: "loop",
			Code: []Instruction{
				{Op: OpSet, Line: 1, Name: "i", Value: 0},
				{Op: OpAdd, Line: 2, Name: "i", Value: 1},
				{Op: OpJump, Line: 3, Value: 1},
			},
		},
		"raise": {
			Name: "raise",
			Code: []Instruction{
				{Op: OpSet, Line: 1, Name: "ready", Value: 1},
				{Op: OpAdd, Line: 2, Name: "ready", Value: 1},
				{Op: OpRaise, Line: 3, Name: "boom"},
			},
		},
	}
}

type Virtual struct {
	program Program

	pauseFlag    atomic.Bool
	shutdownOnce sync.Once

	resume       atomic.Pointer[chan int]
	done         chan struct{}
	shutdown     chan struct{}
	shutdownFlag atomic.Bool
	breakpointMu sync.RWMutex
	breakpoints  map[int]bool
	doneOnce     sync.Once
}

func NewVirtual(program Program) *Virtual {
	return &Virtual{
		program:     program,
		resume:      atomic.Pointer[chan int]{},
		done:        make(chan struct{}),
		shutdown:    make(chan struct{}),
		breakpoints: map[int]bool{},
	}
}

func (v *Virtual) Pause() {
	v.pauseFlag.Store(true)
}

func (v *Virtual) Start(events chan<- Event) { go v.run(events) }

func (v *Virtual) Resume(pauseID int, _ map[int]bool) {
	if resume := v.resume.Load(); resume != nil {
		select {
		case *resume <- pauseID:
		default:
		}
	}
}

func (v *Virtual) UpdateBreakpoints(breakpoints map[int]bool) bool {
	v.breakpointMu.Lock()
	defer v.breakpointMu.Unlock()
	select {
	case <-v.done:
		return false
	default:
	}
	v.breakpoints = copyBreakpoints(breakpoints)
	return true
}

func (v *Virtual) Done() <-chan struct{} { return v.done }

func (v *Virtual) Shutdown() {
	v.shutdownFlag.Store(true)
	v.shutdownOnce.Do(func() { close(v.shutdown) })
}

func (v *Virtual) run(events chan<- Event) {
	vars := map[string]int64{}
	nextPause := 0
	finish := func(reason string) {
		done := false
		v.doneOnce.Do(func() {
			events <- Event{Type: EventTerminated, Reason: reason, Vars: cloneVars(vars)}
			close(v.done)
			done = true
		})
		_ = done
	}

	waitAtSafePoint := func(reason string, pc int) bool {
		nextPause++
		currentPause := nextPause
		resume := make(chan int, 1)
		v.resume.Store(&resume)
		defer v.resume.Store(nil)
		events <- Event{
			Type:    EventStopped,
			Reason:  reason,
			PauseID: currentPause,
			PC:      pc,
			Line:    v.program.Code[pc].Line,
			Vars:    cloneVars(vars),
		}
	waitForCurrent:
		for {
			select {
			case pauseID := <-resume:
				if pauseID != currentPause {
					continue waitForCurrent
				}
				return true
			case <-v.shutdown:
				finish(ReasonExit)
				return false
			}
		}
	}

	if !waitAtSafePoint(ReasonEntry, 0) {
		return
	}
	pc := 0
	for pc >= 0 && pc < len(v.program.Code) {
		if v.shutdownFlag.Load() {
			finish(ReasonExit)
			return
		}
		time.Sleep(StepInterval)

		ins := v.program.Code[pc]
		if v.pauseFlag.Swap(false) {
			if !waitAtSafePoint(ReasonPause, pc) {
				return
			}
			continue
		}
		if v.hasBreakpoint(ins.Line) {
			if !waitAtSafePoint(ReasonBreakpoint, pc) {
				return
			}
			continue
		}

		switch ins.Op {
		case OpSet:
			vars[ins.Name] = ins.Value
		case OpAdd:
			vars[ins.Name] += ins.Value
		case OpJump:
			if int(ins.Value) != pc {
				pc = int(ins.Value)
				continue
			}
		case OpRaise:
			finish(ReasonException)
			return
		}
		pc++
	}
	finish(ReasonExit)
}

func (v *Virtual) hasBreakpoint(line int) bool {
	v.breakpointMu.RLock()
	defer v.breakpointMu.RUnlock()
	return v.breakpoints[line]
}

func cloneVars(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}

func copyBreakpoints(in map[int]bool) map[int]bool {
	out := make(map[int]bool, len(in))
	for line, enabled := range in {
		if enabled {
			out[line] = true
		}
	}
	return out
}
