# internal/scheduler/executor.go + internal/api

Session 3 delivered two things: concrete executor implementations that replace `NoopExecutor`, and a REST API that lets external callers manage jobs.

## Executor layer

### Executor interface (unchanged)

```go
type Executor interface {
    Run(j store.Job) store.RunRecord
}
```

### Dispatcher

`NewDispatcher()` returns a `*Dispatcher` that implements `Executor`. It routes each job to the right implementation based on `job.Executor`:

| `job.Executor` | Implementation | `job.Payload` format |
|---|---|---|
| `shell` | `ShellExecutor` | shell command, e.g. `"python3 /app/fetch.py"` |
| `http` | `HTTPExecutor` | webhook URL, e.g. `"https://hook.example.com/ingest"` |
| `kafka` | `KafkaExecutor` | `"topic:message"`, e.g. `"market.quotes:ping"` |
| *(anything else)* | — | returns `RunRecord{Status:"error"}` |

Pass `scheduler.NewDispatcher()` to `scheduler.Make` (replaces `NoopExecutor`).

### ShellExecutor

Runs `job.Payload` via `/bin/sh -c`. Captures combined stdout+stderr into `RunRecord.Output`. Default timeout is 10 minutes (override with `ShellExecutor{Timeout: ...}`). Exit code goes into `RunRecord.ExitCode`. On timeout, appends `[timeout]` to output.

### HTTPExecutor

POSTs to `job.Payload` with `Content-Type: application/json` and body:

```json
{ "job_id": "...", "job_name": "...", "triggered_at": "..." }
```

`RunRecord.ExitCode` = HTTP status code. 2xx → `Status: "ok"`, anything else → `Status: "error"`. Default HTTP client timeout is 30 seconds (injectable via `HTTPExecutor{Client: ...}`).

### KafkaExecutor

Stub pending Redpanda wiring in Session 5. With no `Brokers` configured, the call is logged and returns `Status: "ok"` (safe no-op). Once `Brokers` is set the TODO in the code will be replaced with `segmentio/kafka-go` producer logic.

### FireNow (scheduler extension)

```go
func (sc *Scheduler) FireNow(jobID string) error
```

Added to `scheduler.go`. Looks up the job in the store and calls `go sc.fireJob(j, time.Now())` — same path as the tick loop but triggered immediately. Returns an error if the job ID does not exist. The API trigger endpoint calls this.

---

## REST API

### Package: `internal/api`

```
internal/api/
├── server.go      ← interfaces, New(), route setup
├── handlers.go    ← all handlers, request/response types, helpers
└── middleware.go  ← logging middleware
```

### Starting the server

```go
apiServer := api.New(st, sc, rf, ":8080")
go apiServer.ListenAndServe()
```

`New` accepts interface values, not concrete types:

| Parameter | Interface | Satisfied by |
|---|---|---|
| `st` | `jobStore` | `*store.Store` |
| `sc` | `jobScheduler` | `*scheduler.Scheduler` |
| `lc` | `leaderChecker` | `*raft.Raft` |

This keeps the api package independent of the concrete scheduler and raft packages, and allows full testing with fake implementations.

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Create or replace a job |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/jobs/{id}` | Get one job + last 10 run records |
| `DELETE` | `/jobs/{id}` | Remove a job |
| `POST` | `/jobs/{id}/trigger` | Run immediately (non-blocking, 202 Accepted) |
| `GET` | `/cluster` | Raft term + leader status |

### POST /jobs

Request body:

```json
{
  "id": "fetch-quotes",
  "name": "Fetch Live Quotes",
  "cron_expr": "*/1 * * * *",
  "executor": "shell",
  "payload": "python3 /app/jobs/fetch_quotes.py",
  "catchup_policy": "skip"
}
```

- `id` is optional — server generates a random hex ID if omitted
- `cron_expr` is validated server-side; returns 400 on parse error
- `catchup_policy` defaults to `"skip"`
- **`NextRun` is computed server-side** as `cron.Parse(cron_expr).Next(time.Now())` — this is the gap that would have caused the tick loop to skip newly created jobs if NextRun were left zero

Response: `201 Created` with the stored `store.Job` as JSON.

### Error responses

| Code | Meaning |
|---|---|
| 400 | Invalid JSON or bad cron expression |
| 404 | Job not found |
| 409 | Not the leader — writes must go to the leader node |
| 500 | Unexpected error |

### GET /cluster

```json
{ "term": 3, "is_leader": true }
```

Reads from `*raft.Raft.GetState()`. Safe on any node — no Raft writes.

---

## cmd/crond/main.go changes

Two changes from Session 2:

1. `scheduler.NoopExecutor{}` → `scheduler.NewDispatcher()`
2. Added `API_ADDR` env var (optional). If set, the REST API server starts alongside the Raft RPC server.

```
BIND_ADDR=:8001 PEERS=h1:8001,h2:8002,h3:8003 ME=0 API_ADDR=:8080 ./crond
```

| Env var | Required | Description |
|---|---|---|
| `BIND_ADDR` | yes | Raft RPC TCP address |
| `PEERS` | yes | Comma-separated peer list (all nodes, same order everywhere) |
| `ME` | yes | 0-indexed position of this node in PEERS |
| `API_ADDR` | no | HTTP address for REST API; omit to disable |

---

## Testing approach

### Executor tests (`executor_test.go`)

- Shell: success (`echo hello`), failure (`exit 42`), timeout (100ms + `sleep 10`), timestamps
- HTTP: success (httptest.Server → 200), non-2xx (500), unreachable URL
- Kafka: no-brokers skip, bad payload format
- Dispatcher: routes to shell, unknown executor → error

### API tests (`api_test.go`)

All tests use `httptest.NewRecorder` + handler calls directly — no network required.

Three fakes inject into the `Server` struct:

| Fake | Replaces |
|---|---|
| `fakeStore` | `*store.Store` — applies Cmds in-memory, no Raft |
| `fakeScheduler` | `*scheduler.Scheduler` — records `FireNow` calls |
| `fakeLeader` | `*raft.Raft` — returns configurable term + leader flag |
