# raft-scheduler

A distributed cron scheduler built on Raft consensus. Only the elected leader fires jobs, preventing duplicate execution across nodes. Followers idle until leadership changes.

## Architecture

```
cmd/crond/          → node entrypoint
internal/raft/      → Raft consensus (election, log replication, snapshots)
internal/store/     → job store backed by Raft (Submit goes through consensus)
internal/scheduler/ → tick loop, executor dispatch, missed-job reconciliation
internal/api/       → REST API (HTTP)
pkg/cron/           → cron expression parser
```

## Executors

Each job has an `executor` field that determines how it runs:

| Executor | Payload format | Behaviour |
|----------|---------------|-----------|
| `shell`  | any shell command | runs via `/bin/sh -c`, 10 min timeout |
| `http`   | URL | POSTs a JSON envelope `{job_id, job_name, triggered_at}` |
| `kafka`  | `topic:message` | publishes to Redpanda/Kafka (stub — wired in Session 5) |

## REST API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | create a job |
| `GET` | `/jobs` | list all jobs |
| `GET` | `/jobs/{id}` | get job + run history |
| `DELETE` | `/jobs/{id}` | delete a job |
| `POST` | `/jobs/{id}/trigger` | run immediately |
| `GET` | `/cluster` | node term and leader status |

### Create a job

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"name":"ping","cron_expr":"* * * * *","executor":"shell","payload":"echo hello"}'
```

## Running

### Single node (Docker)

```sh
docker build -t raft-scheduler .
docker run -p 8080:8080 -p 8001:8001 raft-scheduler
```

### Three-node cluster

```sh
# node 0
docker run -e BIND_ADDR=:8001 -e PEERS=node0:8001,node1:8002,node2:8003 -e ME=0 -e API_ADDR=:8080 -p 8080:8080 -p 8001:8001 raft-scheduler

# node 1
docker run -e BIND_ADDR=:8002 -e PEERS=node0:8001,node1:8002,node2:8003 -e ME=1 -e API_ADDR=:8081 -p 8081:8080 -p 8002:8001 raft-scheduler

# node 2
docker run -e BIND_ADDR=:8003 -e PEERS=node0:8001,node1:8002,node2:8003 -e ME=2 -e API_ADDR=:8082 -p 8082:8080 -p 8003:8001 raft-scheduler
```

### Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BIND_ADDR` | yes | `:8001` | TCP address for Raft RPC |
| `PEERS` | yes | `localhost:8001` | comma-separated list of all peer addresses (same order on every node) |
| `ME` | yes | `0` | 0-indexed position of this node in `PEERS` |
| `API_ADDR` | no | `:8080` | HTTP address for the REST API (omit to disable) |

## Tests

```sh
go test ./...
```
