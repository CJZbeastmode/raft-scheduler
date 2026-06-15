package scheduler

import (
	"time"

	"market-intel/internal/store"
	"market-intel/pkg/cron"
)

// ReconcileMissedJobs: for new nodes joining in cluster
//
// It should be called once after startup, after the store
// has been restored from snapshot/log replay and a leader has emerged.
// It inspects every job whose NextRun is in the past and acts on CatchupPolicy:
//
//   - "skip"     → advance NextRun to the next future occurrence, do not fire
//   - "run_once" → fire the job exactly once now, then advance NextRun
//
// Only the leader's calls to submit will succeed; followers silently do nothing.
func (sc *Scheduler) ReconcileMissedJobs() {
	now := time.Now()
	for _, j := range sc.st.ListJobs() {
		if j.NextRun.IsZero() || !j.NextRun.Before(now) {
			continue // not missed
		}
		switch j.CatchupPolicy {
		case "skip":
			sc.advanceNextRun(j, now)
		case "run_once":
			go sc.fireJob(j, now)
		}
	}
}

// advanceNextRun computes the next future occurrence and submits a CmdUpsert
// to update NextRun in the store without firing the executor.
func (sc *Scheduler) advanceNextRun(j store.Job, now time.Time) {
	next, ok := nextRun(j.CronExpr, now)
	if !ok {
		return
	}
	j.NextRun = next
	sc.submit(store.Cmd{ //nolint:errcheck
		Type:  store.CmdUpsert,
		ReqID: newReqID(),
		Job:   j,
	})
}

// nextRun parses expr and returns the next occurrence after now.
// Returns false if the expression is invalid.
func nextRun(expr string, now time.Time) (time.Time, bool) {
	e, err := cron.Parse(expr)
	if err != nil {
		return time.Time{}, false
	}
	t := e.Next(now)
	if t.IsZero() {
		return time.Time{}, false
	}
	return t, true
}
