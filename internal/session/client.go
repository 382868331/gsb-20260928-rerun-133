package session

import (
	"crypto/rand"
	"encoding/hex"
)

// newToken generates an unguessable client token.
func newToken() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

// Attach registers clientID with the session, or re-attaches a client that
// disconnected earlier: the same client id always receives the same token
// and keeps its role. The first client to attach becomes the controller;
// later clients join as read-only observers. The response always carries a
// fresh snapshot, so a reconnecting client immediately learns the current
// pause or, after exit, the trusted termination reason and last pause.
func (s *Session) Attach(clientID string) AttachResult {
	result := make(chan AttachResult, 1)
	s.submit(func(s *Session) {
		info, ok := s.clients[clientID]
		if !ok {
			info = &clientInfo{token: newToken()}
			s.clients[clientID] = info
			if s.controller == "" {
				s.controller = clientID
			}
		}
		role := RoleObserver
		if s.controller == clientID {
			role = RoleController
		}
		result <- AttachResult{Token: info.token, Role: role, Snapshot: s.snapshotLocked()}
	})
	return <-result
}

// Release drops execution control. Only the controller's own token may
// release; afterwards the session has no controller and any attached client
// can claim it. The releasing client stays attached as an observer.
func (s *Session) Release(token string) error {
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if s.controller == "" {
			errCh <- ErrNoController
			return
		}
		info := s.clients[s.controller]
		if info == nil || info.token != token || token == "" {
			errCh <- ErrNotController
			return
		}
		s.controller = ""
		errCh <- nil
	})
	return <-errCh
}

// Claim hands execution control to the client identified by token when
// control is currently free (no controller, e.g. after a release). It is a
// no-op when the caller already holds control.
func (s *Session) Claim(token string) error {
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if token == "" {
			errCh <- ErrUnknownClient
			return
		}
		var claimant string
		for id, info := range s.clients {
			if info.token == token {
				claimant = id
				break
			}
		}
		if claimant == "" {
			errCh <- ErrUnknownClient
			return
		}
		switch {
		case s.controller == claimant:
			errCh <- nil
		case s.controller != "":
			errCh <- ErrControlHeld
		default:
			s.controller = claimant
			errCh <- nil
		}
	})
	return <-errCh
}
