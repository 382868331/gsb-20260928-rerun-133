package session

import (
	"errors"
	"sort"

	"debugservice/internal/executor"
)

var (
	ErrCandidateStale   = errors.New("candidate needs re-verification before adoption")
	ErrUnknownCandidate = errors.New("unknown breakpoint candidate")
)

// Candidate lifecycle states.
const (
	CandidatePending    = "pending"
	CandidateAdopted    = "adopted"
	CandidateSuperseded = "superseded"
)

// Recheck reasons.
const (
	RecheckNewPause      = "new-pause"
	RecheckSourceChanged = "source-changed"
)

// BreakpointSpec is one breakpoint in a version or candidate: location,
// enable state, an optional condition expression, and an optional
// hit-count threshold. HitCount <= 1 stops on every passing hit;
// HitCount N stops only on the N-th passing hit.
type BreakpointSpec struct {
	Line      int    `json:"line"`
	Enabled   bool   `json:"enabled"`
	Condition string `json:"condition,omitempty"`
	HitCount  int    `json:"hitCount,omitempty"`
}

// BreakpointStatus pairs a spec with the hit count and condition diagnostic
// recorded while its version was in effect.
type BreakpointStatus struct {
	BreakpointSpec
	Hits           int    `json:"hits"`
	ConditionError string `json:"conditionError,omitempty"`
}

// VersionRecord is one adopted breakpoint version. EffectivePauseID names
// the pause whose continue first ran with this version, so every pause can
// be checked against the version that actually produced it. Hits archives
// the per-line hit counts accumulated while the version was active.
type VersionRecord struct {
	ID               int              `json:"id"`
	Breakpoints      []BreakpointSpec `json:"breakpoints"`
	AdoptedBy        string           `json:"adoptedBy"`
	EffectivePauseID int              `json:"effectivePauseId,omitempty"`
	Hits             map[int]int      `json:"hits,omitempty"`
}

// Recheck flags the locations and hit records of a candidate that a source
// change or a new pause made uncertain. A flagged candidate cannot be
// adopted silently; it must be refreshed or re-staged.
type Recheck struct {
	Reason   string `json:"reason"`
	Lines    []int  `json:"lines,omitempty"`
	HitLines []int  `json:"hitLines,omitempty"`
}

// Candidate is a staged, previewable breakpoint change. Only the controller
// can stage, refresh, or adopt one; observers may view candidates through
// BreakpointsView.
type Candidate struct {
	ID          int              `json:"id"`
	Breakpoints []BreakpointSpec `json:"breakpoints"`
	StagedBy    string           `json:"stagedBy"`
	BasePauseID int              `json:"basePauseId"`
	State       string           `json:"state"`
	Recheck     *Recheck         `json:"recheck,omitempty"`

	stagedHits map[int]int
}

// BreakpointsView is the preview every attached client can read: the
// version in effect, its live hit counts and diagnostics, the staged
// candidates, and the archived version history.
type BreakpointsView struct {
	Version      int                `json:"version"`
	PauseVersion int                `json:"pauseVersion"`
	Current      []BreakpointStatus `json:"current"`
	Candidates   []Candidate        `json:"candidates"`
	History      []VersionRecord    `json:"history"`
}

func cloneSpecs(in []BreakpointSpec) []BreakpointSpec {
	out := make([]BreakpointSpec, len(in))
	copy(out, in)
	return out
}

func cloneHits(in map[int]int) map[int]int {
	out := make(map[int]int, len(in))
	for line, hits := range in {
		out[line] = hits
	}
	return out
}

func sortedLines(lines map[int]bool) []int {
	out := make([]int, 0, len(lines))
	for line := range lines {
		out = append(out, line)
	}
	sort.Ints(out)
	return out
}

// executorBreakpoints converts the authoritative specs into the executor's
// installed set, keyed by line.
func executorBreakpoints(specs []BreakpointSpec) map[int]executor.Breakpoint {
	out := make(map[int]executor.Breakpoint, len(specs))
	for _, spec := range specs {
		out[spec.Line] = executor.Breakpoint{
			Line:      spec.Line,
			Enabled:   spec.Enabled,
			Condition: spec.Condition,
			HitCount:  spec.HitCount,
		}
	}
	return out
}

// statusesLocked builds the live status list for the current version.
func (s *Session) statusesLocked() []BreakpointStatus {
	out := make([]BreakpointStatus, 0, len(s.bpSpecs))
	for _, spec := range s.bpSpecs {
		out = append(out, BreakpointStatus{
			BreakpointSpec: spec,
			Hits:           s.bpHits[spec.Line],
			ConditionError: s.bpCondErrs[spec.Line],
		})
	}
	return out
}

// candidateViewLocked copies a candidate for publication.
func candidateViewLocked(c *Candidate) Candidate {
	out := *c
	out.Breakpoints = cloneSpecs(c.Breakpoints)
	out.stagedHits = nil
	if c.Recheck != nil {
		recheck := *c.Recheck
		recheck.Lines = cloneInts(c.Recheck.Lines)
		recheck.HitLines = cloneInts(c.Recheck.HitLines)
		out.Recheck = &recheck
	}
	return out
}

func cloneInts(in []int) []int {
	if in == nil {
		return nil
	}
	out := make([]int, len(in))
	copy(out, in)
	return out
}

// flagCandidatesOnPauseLocked marks every pending candidate whose hit
// records advanced since staging as needing re-verification.
func (s *Session) flagCandidatesOnPauseLocked() {
	for _, c := range s.candidates {
		if c.State != CandidatePending {
			continue
		}
		changed := map[int]bool{}
		for _, spec := range c.Breakpoints {
			if s.bpHits[spec.Line] > c.stagedHits[spec.Line] {
				changed[spec.Line] = true
			}
		}
		if len(changed) == 0 {
			continue
		}
		if c.Recheck == nil {
			c.Recheck = &Recheck{Reason: RecheckNewPause}
		}
		c.Recheck.HitLines = mergeLines(c.Recheck.HitLines, changed)
	}
}

func mergeLines(existing []int, extra map[int]bool) []int {
	seen := map[int]bool{}
	for _, line := range existing {
		seen[line] = true
	}
	for line := range extra {
		seen[line] = true
	}
	return sortedLines(seen)
}

// BreakpointsView returns the preview of versions, candidates, and live
// hit/diagnostic state. Any attached client, including observers, may read
// it; only the controller can change it.
func (s *Session) BreakpointsView() BreakpointsView {
	result := make(chan BreakpointsView, 1)
	s.submit(func(s *Session) {
		view := BreakpointsView{
			Version:      s.bpVersion,
			PauseVersion: s.bpPauseVersion,
			Current:      s.statusesLocked(),
			History:      make([]VersionRecord, 0, len(s.bpHistory)),
		}
		for _, record := range s.bpHistory {
			record.Breakpoints = cloneSpecs(record.Breakpoints)
			record.Hits = cloneHits(record.Hits)
			view.History = append(view.History, record)
		}
		for _, c := range s.candidates {
			view.Candidates = append(view.Candidates, candidateViewLocked(c))
		}
		result <- view
	})
	return <-result
}

// StageCandidate registers a previewable breakpoint change on behalf of the
// controller. The candidate captures the pause and hit records it was
// prepared against, so a later pause or source change can point at exactly
// what must be re-verified. A non-empty requestID makes staging idempotent.
func (s *Session) StageCandidate(clientID string, specs []BreakpointSpec, requestID string) (Candidate, error) {
	type outcome struct {
		candidate Candidate
		err       error
	}
	outCh := make(chan outcome, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.completed[requestID]; ok {
				if stored.err != nil {
					outCh <- outcome{err: stored.err}
				} else {
					outCh <- outcome{candidate: *stored.candidate}
				}
				return
			}
		}
		fail := func(err error) {
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: err}
			}
			outCh <- outcome{err: err}
		}
		if s.state == StateTerminated {
			fail(ErrTerminated)
			return
		}
		if s.controller != "" && clientID != s.controller {
			fail(ErrNotController)
			return
		}
		s.nextCandidateID++
		candidate := &Candidate{
			ID:          s.nextCandidateID,
			Breakpoints: cloneSpecs(specs),
			StagedBy:    clientID,
			BasePauseID: s.pauseID,
			State:       CandidatePending,
			stagedHits:  cloneHits(s.bpHits),
		}
		s.candidates = append(s.candidates, candidate)
		view := candidateViewLocked(candidate)
		if requestID != "" {
			stored := view
			s.completed[requestID] = requestOutcome{candidate: &stored}
		}
		outCh <- outcome{candidate: view}
	})
	out := <-outCh
	return out.candidate, out.err
}

// AdoptCandidate promotes a pending candidate to the authoritative
// breakpoint set as a new version. The version takes effect from the next
// continue (or, while running, at the next safe point). Only the current
// controller may adopt: a late adopt from a displaced controller is
// rejected. A candidate flagged for re-verification cannot be adopted
// silently and must be refreshed first. A non-empty requestID makes the
// adoption idempotent.
func (s *Session) AdoptCandidate(clientID string, candidateID int, requestID string) (VersionRecord, error) {
	type outcome struct {
		record VersionRecord
		err    error
	}
	outCh := make(chan outcome, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.completed[requestID]; ok {
				if stored.err != nil {
					outCh <- outcome{err: stored.err}
				} else {
					outCh <- outcome{record: *stored.version}
				}
				return
			}
		}
		fail := func(err error) {
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: err}
			}
			outCh <- outcome{err: err}
		}
		if s.state == StateTerminated {
			fail(ErrTerminated)
			return
		}
		if s.controller != "" && clientID != s.controller {
			fail(ErrNotController)
			return
		}
		var candidate *Candidate
		for _, c := range s.candidates {
			if c.ID == candidateID {
				candidate = c
				break
			}
		}
		if candidate == nil {
			fail(ErrUnknownCandidate)
			return
		}
		if candidate.State != CandidatePending {
			fail(ErrUnknownCandidate)
			return
		}
		if candidate.Recheck != nil {
			fail(ErrCandidateStale)
			return
		}
		record := s.installVersionLocked(candidate.Breakpoints, clientID)
		candidate.State = CandidateAdopted
		for _, c := range s.candidates {
			if c.State == CandidatePending {
				c.State = CandidateSuperseded
			}
		}
		if requestID != "" {
			stored := record
			s.completed[requestID] = requestOutcome{version: &stored}
		}
		outCh <- outcome{record: record}
	})
	out := <-outCh
	return out.record, out.err
}

// installVersionLocked replaces the authoritative set with a new version.
// The hits of the outgoing version are archived into the history so a later
// review can tell hits that already happened from modifications that were
// still only candidates. The new version is effective from the next
// continue; while running it is pushed to the executor at the next safe
// point.
func (s *Session) installVersionLocked(specs []BreakpointSpec, adoptedBy string) VersionRecord {
	s.bpHistory = append(s.bpHistory, VersionRecord{
		ID:               s.bpVersion,
		Breakpoints:      cloneSpecs(s.bpSpecs),
		AdoptedBy:        s.bpAdoptedBy,
		EffectivePauseID: s.bpEffectivePause,
		Hits:             cloneHits(s.bpHits),
	})
	s.bpVersion++
	s.bpSpecs = cloneSpecs(specs)
	s.bpAdoptedBy = adoptedBy
	s.bpEffectivePause = s.pauseID
	s.bpHits = map[int]int{}
	s.bpCondErrs = map[int]string{}
	if s.state == StateRunning {
		s.bpPendingVersion = s.bpVersion
		s.engine.UpdateBreakpoints(executorBreakpoints(s.bpSpecs))
	}
	return VersionRecord{
		ID:               s.bpVersion,
		Breakpoints:      cloneSpecs(s.bpSpecs),
		AdoptedBy:        adoptedBy,
		EffectivePauseID: s.pauseID,
	}
}

// RefreshCandidate re-bases a flagged candidate on the current pause to clear
// its re-verification flag after the locations and hit records were checked.
// Only the controller may refresh.
func (s *Session) RefreshCandidate(clientID string, candidateID int) (Candidate, error) {
	type outcome struct {
		candidate Candidate
		err       error
	}
	outCh := make(chan outcome, 1)
	s.submit(func(s *Session) {
		if s.state == StateTerminated {
			outCh <- outcome{err: ErrTerminated}
			return
		}
		if s.controller != "" && clientID != s.controller {
			outCh <- outcome{err: ErrNotController}
			return
		}
		for _, c := range s.candidates {
			if c.ID == candidateID && c.State == CandidatePending {
				c.Recheck = nil
				c.BasePauseID = s.pauseID
				c.stagedHits = cloneHits(s.bpHits)
				outCh <- outcome{candidate: candidateViewLocked(c)}
				return
			}
		}
		outCh <- outcome{err: ErrUnknownCandidate}
	})
	out := <-outCh
	return out.candidate, out.err
}

// NotifySourceChange reports that the source under debug changed. Every
// pending candidate is flagged with the locations that no longer match
// validLines (all of them when validLines is empty) and the hit records
// collected since staging, so nothing prepared against the old source is
// applied silently. Only the controller may report a source change.
func (s *Session) NotifySourceChange(clientID string, validLines []int) error {
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if s.state == StateTerminated {
			errCh <- ErrTerminated
			return
		}
		if s.controller != "" && clientID != s.controller {
			errCh <- ErrNotController
			return
		}
		valid := map[int]bool{}
		for _, line := range validLines {
			valid[line] = true
		}
		for _, c := range s.candidates {
			if c.State != CandidatePending {
				continue
			}
			stale := map[int]bool{}
			for _, spec := range c.Breakpoints {
				if !valid[spec.Line] {
					stale[spec.Line] = true
				}
			}
			hitLines := map[int]bool{}
			for _, spec := range c.Breakpoints {
				if s.bpHits[spec.Line] > c.stagedHits[spec.Line] {
					hitLines[spec.Line] = true
				}
			}
			if c.Recheck == nil {
				c.Recheck = &Recheck{}
			}
			c.Recheck.Reason = RecheckSourceChanged
			c.Recheck.Lines = mergeLines(c.Recheck.Lines, stale)
			c.Recheck.HitLines = mergeLines(c.Recheck.HitLines, hitLines)
		}
		errCh <- nil
	})
	return <-errCh
}
