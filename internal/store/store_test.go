package store

import (
	"fmt"
	"testing"
	"time"

	"market-intel/internal/raft"
)

// --- test helpers ---

// newTestStore creates a Store with no Raft backing.
// The returned channel is the applyCh — push ApplyMsg to it to simulate commits.
func newTestStore() (*Store, chan raft.ApplyMsg) {
	ch := make(chan raft.ApplyMsg, 64)
	s := Make(nil, nil, ch)
	return s, ch
}

// feed pushes one command into ch as if Raft had committed it at index.
func feed(ch chan raft.ApplyMsg, index int, cmd Cmd) {
	ch <- raft.ApplyMsg{
		CommandValid: true,
		Command:      cmd,
		CommandIndex: index,
	}
}

// waitFor polls f() every 5ms until it returns true or 2s elapse.
// Fails the test on timeout.
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

func TestUpsertAndGet(t *testing.T) {
	s, ch := newTestStore()

	j := Job{
		ID:       "job-1",
		Name:     "test job",
		CronExpr: "* * * * *",
		Executor: "shell",
		Payload:  "echo hello",
	}
	feed(ch, 1, Cmd{Type: CmdUpsert, ReqID: "r1", Job: j})

	waitFor(t, func() bool { _, ok := s.GetJob("job-1"); return ok })

	got, ok := s.GetJob("job-1")
	if !ok {
		t.Fatal("job not found after upsert")
	}
	if got.Name != "test job" {
		t.Errorf("name: got %q, want %q", got.Name, "test job")
	}
	if got.Executor != "shell" {
		t.Errorf("executor: got %q, want %q", got.Executor, "shell")
	}
}

func TestUpsertUpdates(t *testing.T) {
	// second upsert with the same ID should overwrite, not duplicate
	s, ch := newTestStore()

	feed(ch, 1, Cmd{Type: CmdUpsert, ReqID: "r1", Job: Job{ID: "job-2", Name: "first", Executor: "shell"}})
	waitFor(t, func() bool { _, ok := s.GetJob("job-2"); return ok })

	feed(ch, 2, Cmd{Type: CmdUpsert, ReqID: "r2", Job: Job{ID: "job-2", Name: "updated", Executor: "shell"}})
	waitFor(t, func() bool {
		got, ok := s.GetJob("job-2")
		return ok && got.Name == "updated"
	})

	if n := len(s.ListJobs()); n != 1 {
		t.Errorf("expected 1 job after double upsert, got %d", n)
	}
}

func TestDelete(t *testing.T) {
	s, ch := newTestStore()

	feed(ch, 1, Cmd{Type: CmdUpsert, ReqID: "r1", Job: Job{ID: "job-3", Executor: "shell"}})
	waitFor(t, func() bool { _, ok := s.GetJob("job-3"); return ok })

	feed(ch, 2, Cmd{Type: CmdDelete, ReqID: "r2", ID: "job-3"})
	waitFor(t, func() bool { _, ok := s.GetJob("job-3"); return !ok })

	if _, ok := s.GetJob("job-3"); ok {
		t.Error("job still present after delete")
	}
}

func TestDeleteClearsRuns(t *testing.T) {
	// run history for a deleted job should be cleaned up
	s, ch := newTestStore()

	feed(ch, 1, Cmd{Type: CmdUpsert, ReqID: "r1", Job: Job{ID: "job-del-runs", Executor: "shell"}})
	waitFor(t, func() bool { _, ok := s.GetJob("job-del-runs"); return ok })

	feed(ch, 2, Cmd{Type: CmdRecord, ReqID: "r2", Run: RunRecord{JobID: "job-del-runs", Status: "ok", StartedAt: time.Now()}})
	waitFor(t, func() bool { return len(s.GetRuns("job-del-runs")) == 1 })

	feed(ch, 3, Cmd{Type: CmdDelete, ReqID: "r3", ID: "job-del-runs"})
	waitFor(t, func() bool { _, ok := s.GetJob("job-del-runs"); return !ok })

	if runs := s.GetRuns("job-del-runs"); len(runs) != 0 {
		t.Errorf("expected runs cleared after delete, got %d", len(runs))
	}
}

func TestRecordRun(t *testing.T) {
	s, ch := newTestStore()

	feed(ch, 1, Cmd{Type: CmdUpsert, ReqID: "r1", Job: Job{ID: "job-4", Executor: "shell"}})
	waitFor(t, func() bool { _, ok := s.GetJob("job-4"); return ok })

	now := time.Now()
	run := RunRecord{
		JobID:      "job-4",
		StartedAt:  now,
		FinishedAt: now.Add(100 * time.Millisecond),
		ExitCode:   0,
		Status:     "ok",
		Output:     "hello\n",
	}
	feed(ch, 2, Cmd{Type: CmdRecord, ReqID: "r2", Run: run})

	// 1, job fields updated
	waitFor(t, func() bool {
		got, ok := s.GetJob("job-4")
		return ok && got.LastStatus == "ok"
	})

	got, _ := s.GetJob("job-4")
	if got.LastStatus != "ok" {
		t.Errorf("LastStatus: got %q, want %q", got.LastStatus, "ok")
	}
	if got.LastRun.IsZero() {
		t.Error("LastRun not set after record")
	}

	// 2, run history populated
	runs := s.GetRuns("job-4")
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	if runs[0].Output != "hello\n" {
		t.Errorf("output: got %q", runs[0].Output)
	}
}

func TestRunHistoryCap(t *testing.T) {
	// submitting more than maxRunsPerJob runs should cap, not grow without bound
	s, ch := newTestStore()

	feed(ch, 1, Cmd{Type: CmdUpsert, ReqID: "r1", Job: Job{ID: "job-5", Executor: "shell"}})
	waitFor(t, func() bool { _, ok := s.GetJob("job-5"); return ok })

	total := maxRunsPerJob + 5
	for i := 0; i < total; i++ {
		feed(ch, 2+i, Cmd{
			Type:  CmdRecord,
			ReqID: fmt.Sprintf("r-%d", i),
			Run:   RunRecord{JobID: "job-5", StartedAt: time.Now(), Status: "ok"},
		})
	}

	// sentinel confirms all items drained before we check
	sentinelIdx := 2 + total
	feed(ch, sentinelIdx, Cmd{Type: CmdUpsert, Job: Job{ID: "sentinel-cap"}})
	waitFor(t, func() bool { _, ok := s.GetJob("sentinel-cap"); return ok })

	if got := len(s.GetRuns("job-5")); got != maxRunsPerJob {
		t.Errorf("expected cap of %d runs, got %d", maxRunsPerJob, got)
	}
}

func TestListJobs(t *testing.T) {
	s, ch := newTestStore()

	for i := 0; i < 3; i++ {
		feed(ch, i+1, Cmd{Type: CmdUpsert, Job: Job{ID: fmt.Sprintf("lj-%d", i), Executor: "shell"}})
	}
	waitFor(t, func() bool { return len(s.ListJobs()) == 3 })

	if n := len(s.ListJobs()); n != 3 {
		t.Errorf("expected 3 jobs, got %d", n)
	}
}

func TestSnapshotRoundtrip(t *testing.T) {
	// take a snapshot and restore it into a fresh store — state must match
	s, ch := newTestStore()

	feed(ch, 1, Cmd{Type: CmdUpsert, Job: Job{ID: "snap-1", Name: "job one", Executor: "shell"}})
	feed(ch, 2, Cmd{Type: CmdUpsert, Job: Job{ID: "snap-2", Name: "job two", Executor: "http"}})
	feed(ch, 3, Cmd{Type: CmdRecord, Run: RunRecord{JobID: "snap-1", Status: "ok", StartedAt: time.Now()}})
	waitFor(t, func() bool { return len(s.ListJobs()) == 2 && len(s.GetRuns("snap-1")) == 1 })

	// encode
	s.mu.Lock()
	data := s.encodeSnapshot()
	s.mu.Unlock()

	// restore into a fresh store (no apply loop involvement — direct install)
	s2, _ := newTestStore()
	s2.mu.Lock()
	s2.installSnapshot(data)
	s2.mu.Unlock()

	if _, ok := s2.GetJob("snap-1"); !ok {
		t.Error("snap-1 missing after restore")
	}
	if _, ok := s2.GetJob("snap-2"); !ok {
		t.Error("snap-2 missing after restore")
	}
	if runs := s2.GetRuns("snap-1"); len(runs) != 1 {
		t.Errorf("expected 1 run after restore, got %d", len(runs))
	}
}

func TestSnapshotApplyMsg(t *testing.T) {
	// SnapshotValid msg in applyCh should replace state, not crash
	s1, ch1 := newTestStore()

	feed(ch1, 1, Cmd{Type: CmdUpsert, Job: Job{ID: "pre-snap", Executor: "shell"}})
	waitFor(t, func() bool { _, ok := s1.GetJob("pre-snap"); return ok })

	s1.mu.Lock()
	data := s1.encodeSnapshot()
	s1.mu.Unlock()

	// fresh store receives snapshot via applyCh (simulates InstallSnapshot RPC path)
	s2, ch2 := newTestStore()
	ch2 <- raft.ApplyMsg{SnapshotValid: true, Snapshot: data}
	waitFor(t, func() bool { _, ok := s2.GetJob("pre-snap"); return ok })

	if _, ok := s2.GetJob("pre-snap"); !ok {
		t.Error("job not present after snapshot apply msg")
	}
}

func TestPendingWaiterMismatch(t *testing.T) {
	// if a different command is committed at the pending index, Submit gets ErrNotLeader
	s, ch := newTestStore()

	// manually place a pending entry at index 99 expecting reqID "mine"
	resultCh := make(chan error, 1)
	s.mu.Lock()
	s.pending[99] = pendingEntry{ch: resultCh, reqID: "mine"}
	s.mu.Unlock()

	// commit a different command at that index
	feed(ch, 99, Cmd{Type: CmdUpsert, ReqID: "theirs", Job: Job{ID: "other", Executor: "shell"}})

	select {
	case err := <-resultCh:
		if err != ErrNotLeader {
			t.Errorf("expected ErrNotLeader on mismatch, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mismatch notification")
	}
}
