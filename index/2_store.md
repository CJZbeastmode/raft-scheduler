# internal/store

The replicated job store. A state machine on top of Raft: all writes commit through the log, reads are served locally from in-memory state.

## General Process Description

**1. You call Upsert("daily-report")**

Somewhere above this layer, an API handler builds a `Cmd{Type: CmdUpsert, Job: Job{ID: "daily-report", ...}}` and calls `Submit(cmd)`.

**2. Submit ships it to Raft**

Raft replicates the command to the other nodes in the cluster (say there are 3 nodes). Once 2 of them have written it to their logs, Raft considers it committed. This might take a few milliseconds. Meanwhile, `Submit` is just sitting there blocked on its channel.

**3. Raft fires on applyCh**

Once committed, Raft sends the entry down `applyCh` on every node — the leader and the followers. Every node's `applyLoop` picks it up.

**4. apply() runs on every node**

Each node calls `apply()`, which does `s.jobs["daily-report"] = &job`. Now all three nodes have the same job in memory.

**5. The leader's Submit unblocks**

On the leader node specifically, `apply()` finds the pending waiter and sends `nil` on the channel. `Submit` returns success to the caller.

---

So the key insight: the write doesn't actually happen when you call `Submit`. It happens later, when `apply()` runs — and crucially, it runs on **every node** from the same log, which is how they all stay in sync. The store is basically just those two maps, and Raft is the mechanism that makes every node build those maps identically.

## What was done

- Defined `Job`, `RunRecord`, and `Cmd` — the types that flow through the Raft log
- Implemented `Store.Submit()` — sends a command via `rf.Start()`, blocks until the entry commits (or times out / loses leadership)
- Implemented `Store.applyLoop()` — drains `applyCh`, applies each committed `Cmd`, notifies the blocked Submit waiter
- Snapshot support: `encodeSnapshot()` / `installSnapshot()` round-trip state via gob; triggered automatically every 100 applies via `rf.Snapshot()`

## Files

```
internal/store/
├── store.go      ← types, Make(), Submit(), apply(), applyLoop()
└── snapshot.go   ← encodeSnapshot(), installSnapshot()
```

## What the store provides

```go
// create
s := store.Make(rf, persister, applyCh)

// write (leader only — flows through Raft)
err := s.Submit(store.Cmd{Type: store.CmdUpsert, ReqID: uuid, Job: job})
err := s.Submit(store.Cmd{Type: store.CmdDelete, ReqID: uuid, ID: jobID})
err := s.Submit(store.Cmd{Type: store.CmdRecord, ReqID: uuid, Run: runRecord})

// read (any node — local state)
job, ok := s.GetJob(id)
jobs    := s.ListJobs()
runs    := s.GetRuns(jobID)
```

## Command types

| Type | Effect |
|---|---|
| `CmdUpsert` | Create or overwrite a job. Idempotent by `Job.ID`. |
| `CmdDelete` | Remove job and its run history. |
| `CmdRecord` | Update `Job.LastRun` + `Job.LastStatus`, append to run history. |

## Submit flow

```
caller              Store               Raft
  │                   │                  │
  │  Submit(cmd)      │                  │
  │──────────────────►│                  │
  │                   │  rf.Start(cmd)   │
  │                   │─────────────────►│
  │                   │  (index, term)   │
  │                   │◄─────────────────│
  │                   │  pending[index]  │
  │                   │  = {ch, reqID}   │
  │  (blocked)        │                  │
  │                   │    ...commit...  │
  │                   │  applyCh ◄───────│
  │                   │  apply(cmd,idx)  │
  │                   │  pending[idx]    │
  │                   │  <- nil          │
  │  nil / error      │                  │
  │◄──────────────────│                  │
```

If a different command lands at `index` (leader changed between `Start` and commit), Submit receives `ErrNotLeader` — caller retries against the new leader.

## Deduplication

- `CmdUpsert` is idempotent: same `Job.ID` overwrites in place.
- `CmdRecord` runs are appended; history is capped at 10 per job.
- Leadership mismatch is detected via `ReqID` comparison at commit time (see `apply()`).

## Snapshot

`encodeSnapshot()` / `installSnapshot()` use gob to serialize `jobs` and `runs`. Triggered every 100 applies so the Raft log never grows unbounded. On startup, `Make()` restores from the persisted snapshot before starting the apply loop — entries after `lastIncludedIndex` are replayed by Raft's own apply loop.

## Note on gob registration

`Cmd` is stored as `interface{}` in `LogEntry.Command`. The `init()` in `store.go` registers it with gob. Any binary that imports this package gets the registration automatically.
