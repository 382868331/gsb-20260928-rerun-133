package executor

import (
	"fmt"
	"strconv"
	"strings"
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
	breakpoints  BreakpointSet
	hits         map[int]int
	doneOnce     sync.Once
}

func NewVirtual(program Program) *Virtual {
	return &Virtual{
		program:     program,
		resume:      atomic.Pointer[chan int]{},
		done:        make(chan struct{}),
		shutdown:    make(chan struct{}),
		breakpoints: BreakpointSet{Items: map[int]Breakpoint{}},
		hits:        map[int]int{},
	}
}

func (v *Virtual) Pause() {
	v.pauseFlag.Store(true)
}

func (v *Virtual) Start(events chan<- Event) { go v.run(events) }

func (v *Virtual) Resume(pauseID int, _ BreakpointSet) {
	if resume := v.resume.Load(); resume != nil {
		select {
		case *resume <- pauseID:
		default:
		}
	}
}

func (v *Virtual) UpdateBreakpoints(breakpoints BreakpointSet) bool {
	v.breakpointMu.Lock()
	defer v.breakpointMu.Unlock()
	select {
	case <-v.done:
		return false
	default:
	}
	v.breakpoints = copyBreakpointSet(breakpoints)
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

	waitAtSafePoint := func(reason string, pc int, diagnostic string) bool {
		nextPause++
		currentPause := nextPause
		resume := make(chan int, 1)
		v.resume.Store(&resume)
		defer v.resume.Store(nil)
		events <- Event{
			Type:              EventStopped,
			Reason:            reason,
			PauseID:           currentPause,
			PC:                pc,
			Line:              v.program.Code[pc].Line,
			Vars:              cloneVars(vars),
			BreakpointVersion: v.currentVersion(),
			Diagnostic:        diagnostic,
			Hits:              v.hitSnapshot(),
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

	if !waitAtSafePoint(ReasonEntry, 0, "") {
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
			if !waitAtSafePoint(ReasonPause, pc, "") {
				return
			}
			continue
		}
		if stop, diagnostic := v.checkBreakpoint(ins.Line, vars); stop {
			if !waitAtSafePoint(ReasonBreakpoint, pc, diagnostic) {
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

// checkBreakpoint counts a hit for every enabled breakpoint on line, then
// applies the hit-count convention and the condition. A condition that
// fails to evaluate still stops the program and returns a diagnostic; it is
// never silently treated as a miss.
func (v *Virtual) checkBreakpoint(line int, vars map[string]int64) (bool, string) {
	v.breakpointMu.Lock()
	bp, ok := v.breakpoints.Items[line]
	if !ok || !bp.Enabled {
		v.breakpointMu.Unlock()
		return false, ""
	}
	v.hits[line]++
	hits := v.hits[line]
	v.breakpointMu.Unlock()
	if bp.HitCount > 0 && hits%bp.HitCount != 0 {
		return false, ""
	}
	if bp.Condition == "" {
		return true, ""
	}
	okCond, err := EvalCondition(bp.Condition, vars)
	if err != nil {
		return true, fmt.Sprintf("line %d: condition %q: %v", line, bp.Condition, err)
	}
	return okCond, ""
}

func (v *Virtual) currentVersion() int {
	v.breakpointMu.RLock()
	defer v.breakpointMu.RUnlock()
	return v.breakpoints.Version
}

func (v *Virtual) hitSnapshot() map[int]int {
	v.breakpointMu.RLock()
	defer v.breakpointMu.RUnlock()
	out := make(map[int]int, len(v.hits))
	for line, count := range v.hits {
		out[line] = count
	}
	return out
}

func cloneVars(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}

func copyBreakpointSet(in BreakpointSet) BreakpointSet {
	out := BreakpointSet{Version: in.Version, Items: make(map[int]Breakpoint, len(in.Items))}
	for line, bp := range in.Items {
		out.Items[line] = bp
	}
	return out
}

// EvalCondition evaluates a small condition language: a variable name, a
// comparison operator (== != < <= > >=), and an integer literal, for
// example "i >= 3". Anything else is an error, and an unknown variable is
// an error too, so a typo surfaces as a diagnostic instead of a silent
// miss.
func EvalCondition(condition string, vars map[string]int64) (bool, error) {
	fields := strings.Fields(condition)
	if len(fields) != 3 {
		return false, fmt.Errorf("expected \"<var> <op> <number>\"")
	}
	value, ok := vars[fields[0]]
	if !ok {
		return false, fmt.Errorf("unknown variable %q", fields[0])
	}
	want, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid number %q", fields[2])
	}
	switch fields[1] {
	case "==":
		return value == want, nil
	case "!=":
		return value != want, nil
	case "<":
		return value < want, nil
	case "<=":
		return value <= want, nil
	case ">":
		return value > want, nil
	case ">=":
		return value >= want, nil
	}
	return false, fmt.Errorf("unknown operator %q", fields[1])
}
