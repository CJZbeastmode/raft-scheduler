package store

import (
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"market-intel/internal/raft"
)

func init() {
	// Cmd is stored as interface{} in LogEntry.Command — must register before any encode/decode
	gob.Register(Cmd{})
}

var (
	ErrNotLeader = errors.New("not leader")
	ErrTimeout   = errors.New("submit timed out")
)

type CmdType uint8

const (
	CmdUpsert CmdType = iota // create or replace a job
	CmdDelete                // remove a job
	CmdRecord                // record a run result
)

// Cmd is the unit that flows through the Raft log.
// Every mutation to store state goes through here.
type Cmd struct {
	Type  CmdType
	ReqID string    // unique per Submit call; used to match the waiter at commit time
	Job   Job       // CmdUpsert
	ID    string    // CmdDelete target; CmdRecord.JobID
	Run   RunRecord // CmdRecord
}

// Job holds the full spec plus scheduling state for one cron job.
type Job struct {
	ID            string
	Name          string
	CronExpr      string
	Executor      string // "shell" | "http" | "kafka"
	Payload       string // shell command, URL, or kafka topic:message
	CatchupPolicy string // "skip" | "run_once"
	NextRun       time.Time
	LastRun       time.Time
	LastStatus    string // "ok" | "error" | ""
	CreatedAt     time.Time
}

// RunRecord captures the outcome of one job execution.
type RunRecord struct {
	JobID      string
	StartedAt  time.Time
	FinishedAt time.Time
	ExitCode   int
	Output     string
	Status     string // "ok" | "error"
}

const maxRunsPerJob = 10 // cap run history to avoid unbounded growth

// pendingEntry is a waiter registered by Submit while it blocks on commit.
type pendingEntry struct {
	ch    chan error
	reqID string // expected cmd.ReqID; detects a different command committed at the same index
}

// Store is the replicated state machine on top of Raft.
// Reads are served locally. All writes go through Raft.
type Store struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	jobs map[string]*Job
	runs map[string][]RunRecord // capped at maxRunsPerJob per job

	// log index -> waiter; Submit blocks here until the entry commits
	pending map[int]pendingEntry

	// trigger rf.Snapshot() every snapThreshold applies to bound log size
	applyCount    int
	snapThreshold int
	persister     *raft.Persister
}

// Make creates a Store and starts its apply loop.
// rf and persister may be nil (test mode — bypasses Raft).
func Make(rf *raft.Raft, persister *raft.Persister, applyCh chan raft.ApplyMsg) *Store {
	s := &Store{
		rf:            rf,
		persister:     persister,
		applyCh:       applyCh,
		jobs:          make(map[string]*Job),
		runs:          make(map[string][]RunRecord),
		pending:       make(map[int]pendingEntry),
		snapThreshold: 100,
	}

	// restore from snapshot if one exists (crash recovery)
	if persister != nil {
		if snap := persister.ReadSnapshot(); len(snap) > 0 {
			s.installSnapshot(snap)
		}
	}

	go s.applyLoop()
	return s
}

// Submit sends cmd through Raft and waits for it to be committed.
// Only succeeds on the leader. Retries on ErrNotLeader after a re-election.
func (s *Store) Submit(cmd Cmd) error {
	if s.rf == nil {
		return ErrNotLeader
	}

	s.mu.Lock()
	index, _, isLeader := s.rf.Start(cmd)
	// if not leader, return ErrNotLeader immediately without blocking
	if !isLeader {
		s.mu.Unlock()
		return ErrNotLeader
	}
	// if is leader, assigns the command a log index and begins replicating it to peers
	// register a waiter so applyLoop can notify us when the entry commits (or another entry overwrites it)
	ch := make(chan error, 1)
	s.pending[index] = pendingEntry{ch: ch, reqID: cmd.ReqID}
	s.mu.Unlock()

	// wait 5 seconds for commit notification
	// else timeout for caller to retry (e.g. if we lost leadership or a peer is down)
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		// clean up the waiter so apply doesn't notify a dead channel
		s.mu.Lock()
		delete(s.pending, index)
		s.mu.Unlock()
		return ErrTimeout
	}
}

// GetJob returns a copy of the named job, or false if it doesn't exist.
func (s *Store) GetJob(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// ListJobs returns a copy of all known jobs.
func (s *Store) ListJobs() []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, *j)
	}
	return out
}

// GetRuns returns recent run records for a job (newest last, up to maxRunsPerJob).
func (s *Store) GetRuns(jobID string) []RunRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]RunRecord(nil), s.runs[jobID]...)
}

// applyLoop drains applyCh and applies each committed entry.
func (s *Store) applyLoop() {
	for msg := range s.applyCh {
		if msg.SnapshotValid {
			// follower catching up via InstallSnapshot — replace state entirely
			s.mu.Lock()
			s.installSnapshot(msg.Snapshot)
			// fail any pending Submits: we're a follower, not the leader
			for idx, entry := range s.pending {
				entry.ch <- ErrNotLeader
				delete(s.pending, idx)
			}
			s.mu.Unlock()
			continue
		}

		cmd, ok := msg.Command.(Cmd)
		if !ok {
			// not our type (e.g. nil sentinel at log[0])
			continue
		}

		s.mu.Lock()
		s.apply(cmd, msg.CommandIndex)
		s.mu.Unlock()
	}
}

// apply mutates state for one committed command and notifies any waiter.
// Must be called with s.mu held.
func (s *Store) apply(cmd Cmd, index int) {
	switch cmd.Type {
	
	// write or overwrite a job in s.jobs
	case CmdUpsert:
		j := cmd.Job
		s.jobs[j.ID] = &j

	// delete a job and its run history
	case CmdDelete:
		delete(s.jobs, cmd.ID)
		delete(s.runs, cmd.ID)

	// record a run result, update the job's last-run fields, and cap run history
	case CmdRecord:
		r := cmd.Run
		// update the job's last-run fields
		if j, ok := s.jobs[r.JobID]; ok {
			j.LastRun = r.StartedAt
			j.LastStatus = r.Status
		}
		// append and cap run history
		s.runs[r.JobID] = append(s.runs[r.JobID], r)
		if len(s.runs[r.JobID]) > maxRunsPerJob {
			s.runs[r.JobID] = s.runs[r.JobID][len(s.runs[r.JobID])-maxRunsPerJob:]
		}
	}

	// notify the Submit waiter if one is registered at this index
	// delete waiter
	if entry, ok := s.pending[index]; ok {
		delete(s.pending, index)
		if cmd.ReqID == entry.reqID {
			entry.ch <- nil // our command committed successfully
		} else {
			// different command was committed at this slot — we lost leadership between
			// Start() and commit; caller should retry against the new leader
			entry.ch <- ErrNotLeader
		}
	}

	// snapshot when the log has grown too long
	s.applyCount++
	if s.rf != nil && s.applyCount >= s.snapThreshold {
		data := s.encodeSnapshot() // encode while holding mu — snapshot is consistent
		go s.rf.Snapshot(index, data)
		s.applyCount = 0
	}
}
