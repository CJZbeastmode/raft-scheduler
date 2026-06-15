package scheduler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"market-intel/internal/raft"
	"market-intel/internal/store"
)

// --- mocks ---

type mockLeader struct{ leader bool }

func (m mockLeader) GetState() (int, bool) { return 1, m.leader }

// captureExecutor records every job it receives.
type captureExecutor struct {
	mu   sync.Mutex
	jobs []store.Job
}

func (e *captureExecutor) Run(j store.Job) store.RunRecord {
	e.mu.Lock()
	e.jobs = append(e.jobs, j)
	e.mu.Unlock()
	return store.RunRecord{
		JobID:      j.ID,
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
		Status:     "ok",
	}
}

func (e *captureExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.jobs)
}

// --- helpers ---

// newTestScheduler builds a Scheduler wired to an in-process store.
// sc.submit is overridden so commands feed directly into the store's applyCh
// without going through Raft.
func newTestScheduler(isLeader bool) (*Scheduler, *store.Store, chan raft.ApplyMsg, *captureExecutor) {
	ch := make(chan raft.ApplyMsg, 128)
	st := store.Make(nil, nil, ch)
	exec := &captureExecutor{}

	// build scheduler without starting tickLoop (tests call tick/ReconcileMissedJobs directly)
	sc := &Scheduler{
		lc:   mockLeader{isLeader},
		st:   st,
		exec: exec,
		done: make(chan struct{}),
	}

	// submit feeds directly into the store's applyCh at indices starting at 1000
	// so they don't collide with the setup indices below
	var idx int64 = 1000
	sc.submit = func(cmd store.Cmd) error {
		i := int(atomic.AddInt64(&idx, 1))
		ch <- raft.ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: i}
		return nil
	}

	return sc, st, ch, exec
}

// addJob feeds a job directly into the test store (simulates it already being in Raft).
func addJob(ch chan raft.ApplyMsg, idx int, j store.Job) {
	ch <- raft.ApplyMsg{
		CommandValid: true,
		Command:      store.Cmd{Type: store.CmdUpsert, Job: j},
		CommandIndex: idx,
	}
}

// waitFor polls f() every 5ms until true or 2s passes.
func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// --- tests ---

func TestTickFiresOverdueJob(t *testing.T) {
	sc, st, ch, exec := newTestScheduler(true)

	j := store.Job{
		ID:       "j-overdue",
		CronExpr: "* * * * *",
		Executor: "shell",
		Payload:  "echo hi",
		NextRun:  time.Now().Add(-5 * time.Minute), // overdue
	}
	addJob(ch, 1, j)
	waitFor(t, func() bool { _, ok := st.GetJob("j-overdue"); return ok })

	sc.tick(time.Now())

	// 1, executor was called
	waitFor(t, func() bool { return exec.callCount() == 1 })

	// 2, NextRun advanced to the future
	waitFor(t, func() bool {
		got, ok := st.GetJob("j-overdue")
		return ok && got.NextRun.After(time.Now())
	})
}

func TestTickSkipsFutureJob(t *testing.T) {
	sc, st, ch, exec := newTestScheduler(true)

	j := store.Job{
		ID:       "j-future",
		CronExpr: "0 9 * * *",
		Executor: "shell",
		NextRun:  time.Now().Add(10 * time.Minute), // not due yet
	}
	addJob(ch, 1, j)
	waitFor(t, func() bool { _, ok := st.GetJob("j-future"); return ok })

	sc.tick(time.Now())

	time.Sleep(50 * time.Millisecond)
	if exec.callCount() != 0 {
		t.Errorf("executor called %d times, want 0", exec.callCount())
	}
}

func TestTickFiresMultipleDueJobs(t *testing.T) {
	sc, st, ch, exec := newTestScheduler(true)

	past := time.Now().Add(-1 * time.Minute)
	for i, id := range []string{"m-1", "m-2", "m-3"} {
		addJob(ch, i+1, store.Job{
			ID:       id,
			CronExpr: "* * * * *",
			Executor: "shell",
			NextRun:  past,
		})
	}
	waitFor(t, func() bool { return len(st.ListJobs()) == 3 })

	sc.tick(time.Now())

	waitFor(t, func() bool { return exec.callCount() == 3 })
}

func TestReconcileSkipAdvancesNextRun(t *testing.T) {
	// "skip" policy: advance NextRun, do not call executor
	sc, st, ch, _ := newTestScheduler(true)
	exec := sc.exec.(*captureExecutor)

	j := store.Job{
		ID:            "rec-skip",
		CronExpr:      "* * * * *",
		Executor:      "shell",
		CatchupPolicy: "skip",
		NextRun:       time.Now().Add(-30 * time.Minute), // missed while offline
	}
	addJob(ch, 1, j)
	waitFor(t, func() bool { _, ok := st.GetJob("rec-skip"); return ok })

	sc.ReconcileMissedJobs()

	// NextRun moves to the future
	waitFor(t, func() bool {
		got, ok := st.GetJob("rec-skip")
		return ok && got.NextRun.After(time.Now())
	})

	// executor never called
	time.Sleep(30 * time.Millisecond)
	if exec.callCount() != 0 {
		t.Errorf("executor called %d times, want 0 for skip policy", exec.callCount())
	}
}

func TestReconcileRunOnceFiresAndAdvances(t *testing.T) {
	// "run_once" policy: fire once now, advance NextRun
	sc, st, ch, exec := newTestScheduler(true)

	j := store.Job{
		ID:            "rec-run",
		CronExpr:      "* * * * *",
		Executor:      "shell",
		CatchupPolicy: "run_once",
		NextRun:       time.Now().Add(-10 * time.Minute),
	}
	addJob(ch, 1, j)
	waitFor(t, func() bool { _, ok := st.GetJob("rec-run"); return ok })

	sc.ReconcileMissedJobs()

	// executor called exactly once
	waitFor(t, func() bool { return exec.callCount() == 1 })

	// NextRun advanced to the future
	waitFor(t, func() bool {
		got, ok := st.GetJob("rec-run")
		return ok && got.NextRun.After(time.Now())
	})
}

func TestReconcileIgnoresCurrentJobs(t *testing.T) {
	// jobs with NextRun in the future should not be touched by reconcile
	sc, st, ch, exec := newTestScheduler(true)

	j := store.Job{
		ID:            "rec-future",
		CronExpr:      "0 9 * * *",
		Executor:      "shell",
		CatchupPolicy: "run_once",
		NextRun:       time.Now().Add(1 * time.Hour),
	}
	addJob(ch, 1, j)
	waitFor(t, func() bool { _, ok := st.GetJob("rec-future"); return ok })

	sc.ReconcileMissedJobs()

	time.Sleep(30 * time.Millisecond)
	if exec.callCount() != 0 {
		t.Errorf("executor called %d times, want 0 for future job", exec.callCount())
	}
}

func TestFireJobRecordsResult(t *testing.T) {
	// after fireJob, the store should have a RunRecord for the job
	sc, st, ch, _ := newTestScheduler(true)

	j := store.Job{
		ID:       "j-record",
		CronExpr: "* * * * *",
		Executor: "shell",
		NextRun:  time.Now().Add(-1 * time.Minute),
	}
	addJob(ch, 1, j)
	waitFor(t, func() bool { _, ok := st.GetJob("j-record"); return ok })

	sc.tick(time.Now())

	waitFor(t, func() bool {
		return len(st.GetRuns("j-record")) > 0
	})

	runs := st.GetRuns("j-record")
	if runs[0].Status != "ok" {
		t.Errorf("run status: got %q, want %q", runs[0].Status, "ok")
	}
}
