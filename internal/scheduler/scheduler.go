package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"market-intel/internal/store"
)

// leaderChecker is the leadership-query subset of *raft.Raft.
// Separated into an interface so tests can inject a mock without a real cluster.
type leaderChecker interface {
	GetState() (int, bool)
}

// Executor dispatches a job and returns the outcome.
// Implemented per executor type in executor.go (Session 3).
type Executor interface {
	Run(j store.Job) store.RunRecord
}

// NoopExecutor is a placeholder until executor.go is wired in.
type NoopExecutor struct{}

func (NoopExecutor) Run(j store.Job) store.RunRecord {
	return store.RunRecord{
		JobID:      j.ID,
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
		Status:     "ok",
		Output:     "(noop)",
	}
}

// Scheduler drives the cron tick loop.
// Only the leader fires jobs — followers idle in the loop.
type Scheduler struct {
	lc   leaderChecker		// am i the leader?
	st   *store.Store		// store to read jobs from and submit updates to
	exec Executor			// the thing that actually runs a job (kafka, http, etc.)

	// submit wraps st.Submit; field allows tests to bypass Raft without a live cluster
	submit func(store.Cmd) error

	done chan struct{}
}

// Make creates a Scheduler backed by lc (leadership check), st (job store), and exec.
// Accepts any leaderChecker — pass *raft.Raft in production, a mock in tests.
func Make(lc leaderChecker, st *store.Store, exec Executor) *Scheduler {
	sc := &Scheduler{
		lc:   lc,
		st:   st,
		exec: exec,
		done: make(chan struct{}),
	}
	sc.submit = st.Submit
	go sc.tickLoop()
	return sc
}

// Kill stops the scheduler's tick loop.
func (sc *Scheduler) Kill() {
	close(sc.done)
}

// tickLoop ticks every 15s and fires any due jobs.
// 15s granularity means a job fires within one tick of its scheduled minute.
// Only the leader submits — followers just idle here.
func (sc *Scheduler) tickLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sc.done:
			return
		case now := <-ticker.C:
			_, isLeader := sc.lc.GetState()
			if !isLeader {
				continue // wait for the next tick — leadership may change
			}
			sc.tick(now)
		}
	}
}

// tick finds all jobs due at or before now and fires each in its own goroutine.
func (sc *Scheduler) tick(now time.Time) {
	for _, j := range sc.st.ListJobs() {
		if j.NextRun.IsZero() || j.NextRun.After(now) {
			continue
		}
		go sc.fireJob(j, now)
	}
}

// fireJob runs one job: advances NextRun through Raft (prevents double-fire on crash),
// calls the executor, then records the result.
func (sc *Scheduler) fireJob(j store.Job, now time.Time) {
	next, ok := nextRun(j.CronExpr, now)
	if !ok {
		return
	}

	// 1. advance NextRun before firing
	// if we crash after this commit but before the executor runs, recovery
	// sees NextRun in the future and knows the job was scheduled-but-not-run
	j.NextRun = next
	if err := sc.submit(store.Cmd{
		Type:  store.CmdUpsert,
		ReqID: newReqID(),
		Job:   j,
	}); err != nil {
		return // lost leadership between tick and submit — another node will handle it
	}

	// 2. run executor
	run := sc.exec.Run(j)
	run.JobID = j.ID
	if run.StartedAt.IsZero() {
		run.StartedAt = now
	}
	if run.FinishedAt.IsZero() {
		run.FinishedAt = time.Now()
	}

	// 3. record result — best-effort, not fatal if we lose leadership here
	sc.submit(store.Cmd{ //nolint:errcheck
		Type:  store.CmdRecord,
		ReqID: newReqID(),
		Run:   run,
	})
}

// newReqID generates a random 16-hex-char request ID for Raft dedup.
func newReqID() string {
	b := make([]byte, 8)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}
