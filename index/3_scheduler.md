# internal/scheduler

The cron tick engine. Sits above the store: reads job specs, advances `NextRun` through Raft, dispatches executors, records results.

## Files

```
internal/scheduler/
├── scheduler.go  ← tick loop, fireJob, Executor interface, NoopExecutor
├── recovery.go   ← ReconcileMissedJobs, advanceNextRun
└── executor.go   ← stub (Session 3: shell/http/kafka implementations)
```

## What the scheduler provides

```go
// create (pass any leaderChecker — *raft.Raft in production, mock in tests)
sc := scheduler.Make(rf, st, scheduler.NoopExecutor{})

// startup catchup — call once after cluster has a leader
sc.ReconcileMissedJobs()

// shutdown
sc.Kill()
```

The tick loop starts automatically inside `Make()`.

## Tick loop

Fires every 15 seconds. Checks leadership first — only the leader fires jobs.

```
tickLoop()  ← goroutine, every 15s
    GetState() → not leader → skip
    GetState() → leader     → tick(now)
        for each job: NextRun <= now → go fireJob(j, now)
```

15s granularity means a job fires within one tick of its scheduled minute. Fine for cron.

## fireJob flow

```
fireJob(j, now)
    1. parse CronExpr → compute next occurrence
    2. Submit CmdUpsert with NextRun = next   ← Raft commit, prevents double-fire on crash
    3. exec.Run(j)                            ← shell / http / kafka (Session 3)
    4. Submit CmdRecord with result           ← best-effort
```

Step 2 commits before step 3 fires. If the process crashes between 2 and 3, the job's `NextRun` is already in the future. Recovery (below) will not re-fire — the execution is simply lost. This is acceptable for a cron scheduler; for stronger guarantees see the `run_once` catchup policy.

## ReconcileMissedJobs

Call once on startup (after the cluster has a leader) to handle jobs that were due while the system was offline.

| CatchupPolicy | Action |
|---|---|
| `skip` | advance `NextRun` to next future occurrence, do not fire |
| `run_once` | fire exactly once now, then advance `NextRun` |
| *(unset)* | job is ignored by reconcile; tick will pick it up within 15s |

## Executor interface

```go
type Executor interface {
    Run(j store.Job) store.RunRecord
}
```

`NoopExecutor` is the placeholder. Session 3 replaces it with:
- `ShellExecutor` — runs `job.Payload` as a shell command, captures stdout/stderr
- `HTTPExecutor` — POST `job.Payload` as a webhook URL
- `KafkaExecutor` — publish `job.Payload` to a Redpanda topic

## leaderChecker interface

`Make()` accepts `leaderChecker` (has `GetState() (int, bool)`) rather than `*raft.Raft` directly. This keeps the scheduler testable without a real cluster — tests inject `mockLeader{leader: true}`.

## cmd/crond/main.go

The single binary that bootstraps a Raft node, the store, and the scheduler.

```
BIND_ADDR=:8001 PEERS=host1:8001,host2:8002,host3:8003 ME=0 ./crond
```

| Env var | Description |
|---|---|
| `BIND_ADDR` | TCP address this node listens on |
| `PEERS` | Comma-separated list of ALL peers in index order (same on every node) |
| `ME` | This node's 0-indexed position in `PEERS` |

Startup sequence:
1. Parse env, build `[]*raft.Peer`
2. `net.Listen` → start RPC server (`raft.RPCAdapter` registered as `"Raft"`)
3. `raft.Make` → leader election begins
4. `store.Make` → restores from snapshot, starts apply loop
5. `scheduler.Make` → starts tick loop
6. Background goroutine polls until leader, then calls `ReconcileMissedJobs`
7. Block on SIGINT / SIGTERM

**Note on persistence:** `raft.MakePersister()` is currently in-memory. Raft state and snapshots survive the process lifetime but not a crash. Session 8 will replace this with a disk-backed persister.
