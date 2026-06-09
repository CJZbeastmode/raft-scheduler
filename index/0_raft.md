# internal/raft

A production-ready Raft consensus implementation ported from MIT 6.5840 (Labs 3A–3D). Provides leader election, log replication, persistence, and snapshotting over real TCP connections.

## What was done

Took the completed MIT 6.5840 Raft implementation and adapted it for production use:

- Replaced `labgob` (lab encoder) with standard `encoding/gob`
- Replaced `labrpc` (fake in-memory network) with a real TCP transport (`rpc.go`)
- Kept `persister.go` unchanged — it is pure Go with no lab dependencies
- Updated `Raft.peers` field from `[]*labrpc.ClientEnd` to `[]*Peer`
- Updated `Make()` signature to accept `[]*Peer` instead of lab endpoints

## Files

```
internal/raft/
├── raft.go        ← consensus implementation (election, replication, snapshots)
├── rpc.go         ← real TCP transport replacing labrpc
├── persister.go   ← disk persistence for raft state and snapshots
└── util.go        ← DPrintf debug helper
```

## What Raft provides

### Leader election
Randomised election timeouts (250–400ms). Candidates request votes from all peers in parallel. First to reach majority wins. Stale leaders step down automatically when they see a higher term.

### Log replication
Leader appends commands to its log and replicates to followers via `AppendEntries`. Uses accelerated conflict resolution — followers return the conflicting term and first index so the leader can skip backward in bulk rather than one entry at a time.

### Persistence
`currentTerm`, `votedFor`, and the full log are persisted to disk on every state change via `persister.go`. Survives crashes and restarts correctly.

### Snapshots
The layer above (state machine) can call `Snapshot(index, data)` to compact the log. The leader sends snapshots to followers that fall too far behind via `InstallSnapshot` RPC. Snapshot and raft state are saved atomically.

## API

```go
// create a Raft node
rf := raft.Make(peers, me, persister, applyCh)

// submit a command (leader only)
index, term, isLeader := rf.Start(command)

// check state
term, isLeader := rf.GetState()

// compact the log
rf.Snapshot(index, snapshotBytes)

// shut down
rf.Kill()
```

Commands committed by the cluster are delivered in order on `applyCh` as `ApplyMsg` values. The state machine reads from this channel and applies each command.

## Note on gob and interface{}

`LogEntry.Command` is typed as `interface{}`. When the scheduler submits concrete command types (e.g. `JobCommand`), register them once at startup before any encoding:

```go
gob.Register(JobCommand{})
```

Forgetting this causes silent gob decode failures at runtime.