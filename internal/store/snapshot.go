package store

import (
	"bytes"
	"encoding/gob"
	"log"
)

// storeState is the serializable form of the store's state machine.
// Only what needs to survive a snapshot goes here — pending waiters do not.
type storeState struct {
	Jobs map[string]*Job
	Runs map[string][]RunRecord
}

// encodeSnapshot serializes current state for rf.Snapshot().
// Must be called with s.mu held.
func (s *Store) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(storeState{Jobs: s.jobs, Runs: s.runs}); err != nil {
		log.Panicf("store: snapshot encode: %v", err)
	}
	return w.Bytes()
}

// installSnapshot replaces the current state with the decoded snapshot.
// Must be called with s.mu held.
func (s *Store) installSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	var state storeState
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&state); err != nil {
		log.Printf("store: snapshot decode: %v", err)
		return
	}
	s.jobs = state.Jobs
	s.runs = state.Runs
}
