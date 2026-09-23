# Intelligent Distributive Computing Platform (IDCP)

IDCP is a B.Tech project exploring a predictive, risk-aware, self-healing distributed computing platform. This repository implements Stages 1–3 plus **Stage 4A: PostgreSQL and SQLAlchemy foundation** and **Stage 4B: Persistent node metrics and heartbeat history** on a single laptop.

It deliberately does not yet include ML, blockchain, React, workload migration, predictive/risk-aware scheduling, checkpointing, retry, or advanced scheduling. Stage 4A created the initial persistent schema; Stage 4B now persists every real worker heartbeat into PostgreSQL while keeping the proven Stage 1–3 in-memory registries as the scheduling source of truth.

## What Stages 1-3 implement

- A Python/Flask master service with JSON APIs for registration, heartbeats, node listing, and health.
- A reusable Python node agent that collects real `psutil` CPU and memory metrics.
- Three independently running worker containers (`node-01`, `node-02`, and `node-03`).
- In-memory node status tracking and automatic `ONLINE` / `OFFLINE` detection.
- Retry behavior when the master is temporarily unavailable, including recovery after a master restart.
- A real worker-side task API in every node agent, available only inside the Docker network.
- In-memory task lifecycle tracking and asynchronous task dispatch, so task execution does not block heartbeats.
- A simple `ONLINE` + `IDLE` round-robin scheduler.
- One real computation type: `sum_range`.
- Stage 3 splitting of one `sum_range` parent task into up to three real worker subtasks.
- Master-side aggregation of only the results returned by those workers.

`available_memory` is reported in bytes, exactly as provided by `psutil`.

## Architecture

```text
Windows laptop (Docker Desktop + WSL2)
            |
       Docker bridge network: idcp-network
            |
  +---------+---------+---------+---------+
  |         |         |         |         |
master   postgres  node-01   node-02   node-03
Flask    PostgreSQL Node Agent + Task API :5001
 :5000     :5432    (same code in each container)
```

Each agent calls `http://master:5000` through Docker's service-name DNS; it does not use `localhost` or a fixed IP address. The master sends work to the selected agent using its service-name URL, such as `http://node-01:5001/api/tasks/execute`, and connects to PostgreSQL through `postgres:5432`. Only the master is published to the Windows host at `http://localhost:5000`.

The three worker containers are **logical worker nodes**, not three physical computers. They share the host laptop's underlying CPU and RAM, although each runs as an independent process/container and reports the metrics visible within its own container. Later, run the same `node-agent` image or Python program on separate machines/VMs and set `MASTER_URL` to the reachable master address; no change to the agent's collection or heartbeat logic is required.

### Stage 3 execution flow

```text
POST /api/tasks: sum_range(1..1,000,000)
                  |
                  v
        Master: split into contiguous subranges
                  |
     +------------+------------+
     |            |            |
 node-01       node-02       node-03
 1..333334   333335..666667  666668..1000000
     |            |            |
     +------------+------------+
                  |
                  v
       Master aggregates returned partial results
                  |
                  v
            final_result = 500000500000
```

## Prerequisites

- Windows 10/11 with Docker Desktop installed and running.
- WSL2 backend enabled in Docker Desktop.
- Docker Compose v2 (`docker compose version`).

## Start the platform

From the repository root in PowerShell:

```powershell
Copy-Item .env.example .env
# Edit .env and replace POSTGRES_PASSWORD before continuing.
docker compose config
docker compose up --build
```

Leave this terminal open to follow logs. The master starts first; agents retry until it is ready, register, and then send a heartbeat every five seconds.

To run it in the background:

```powershell
docker compose up --build -d
```

## Stop the platform

```powershell
docker compose down
```

This stops the containers and removes the network. PostgreSQL data remains in the named `postgres-data` volume. To remove it too, use `docker compose down -v`.

## View logs

```powershell
docker compose logs -f
docker compose logs -f master
docker compose logs -f node-01
```

## Test the APIs

After about 10 seconds, all three nodes should be `ONLINE`:

```powershell
Invoke-RestMethod http://localhost:5000/api/health
Invoke-RestMethod http://localhost:5000/api/nodes | ConvertTo-Json -Depth 5
```

The available APIs are:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/api/nodes/register` | Register or refresh a node |
| `POST` | `/api/nodes/heartbeat` | Submit real node metrics (persisted) |
| `GET` | `/api/nodes` | List known nodes and live status |
| `GET` | `/api/nodes/<node_id>` | Node detail with latest persisted metric |
| `GET` | `/api/nodes/<node_id>/metrics` | Heartbeat history for one node |
| `GET` | `/api/health` | Check master health and node counts |
| `POST` | `/api/tasks` | Create and asynchronously dispatch a workload |
| `GET` | `/api/tasks` | List all known workloads |
| `GET` | `/api/tasks/<task_id>` | Retrieve one workload and its result |

## Stage 2: single-worker workload execution

Stage 2 adds one meaningful computation:

```json
{
  "task_type": "sum_range",
  "start": 1,
  "end": 1000000
}
```

The selected node performs `sum(range(1, 1000001))` using Python integer arithmetic and returns `500000500000`; the result is not fabricated by the master.

With one available node, the task lifecycle is:

```text
CREATED -> ASSIGNED -> RUNNING -> COMPLETED
                         |
                         +-> FAILED
```

The POST response always initially reports `CREATED`. A background dispatch thread selects the next `ONLINE` and `IDLE` node in round-robin order, marks it `BUSY`, sends it the task, stores the real result, and returns that node to `IDLE`. `GET /api/nodes` includes this simple node `task_state` alongside the existing Stage 1 metrics.

Submit and retrieve a workload from PowerShell:

```powershell
$body = @{ task_type = "sum_range"; start = 1; end = 1000000 } | ConvertTo-Json
$created = Invoke-RestMethod -Method Post -Uri http://localhost:5000/api/tasks -ContentType "application/json" -Body $body
$created
Invoke-RestMethod "http://localhost:5000/api/tasks/$($created.task_id)" | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:5000/api/tasks | ConvertTo-Json -Depth 5
```

Invalid requests are rejected with JSON errors: missing/unsupported `task_type`, non-integer `start` or `end`, and `start > end` are all invalid.

## Stage 3: task splitting and aggregation

Stage 3 extends the same `POST /api/tasks` request. It claims up to three real `ONLINE` and `IDLE` workers and splits the inclusive range into balanced, contiguous, non-overlapping subranges. If fewer workers are available, it uses the available number; if none are available, the parent task fails cleanly. A range shorter than the worker count is split only into non-empty subtasks.

For `1..1000000` on three workers, the deterministic splitter produces:

| Worker | Subtask range |
| --- | --- |
| `node-01` | `1..333334` |
| `node-02` | `333335..666667` |
| `node-03` | `666668..1000000` |

For an uneven range, extra values go to earlier chunks: `1..10` across three workers becomes `1..4`, `5..7`, and `8..10`. This guarantees no gap and no overlap.

The worker remains unaware of the parent task. It calculates only its assigned `sum_range` subtask. The master records each returned partial result and aggregates those values; it never recalculates the original parent range as a shortcut.

Distributed task lifecycle:

```text
CREATED -> SPLIT -> subtask ASSIGNED/RUNNING -> AGGREGATING -> COMPLETED
                            |
                            +---------------------------> FAILED
```

`GET /api/tasks/<task_id>` now includes `total_subtasks`, `completed_subtasks`, `failed_subtasks`, `subtasks`, and `final_result`. The existing Stage 2 `result` field is preserved as an alias for `final_result`, and a one-worker task produces exactly one subtask.

Stage 3 demonstrates real distributed computation, but does **not** add task retries, migration, checkpointing, self-healing, ML, prediction, or risk-aware scheduling.

## Stage 4A: PostgreSQL and SQLAlchemy foundation

Stage 4A adds a PostgreSQL 16 service and a small Flask-SQLAlchemy model layer. The master receives `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` from Docker Compose and builds its database URL with the `postgres` service name, never `localhost`.

At startup, the master retries the database connection while PostgreSQL initializes, then calls `db.create_all()` to create the initial `nodes` table. The [Node model](backend/models.py) contains persistent node identity, endpoint, registration, heartbeat, and audit timestamp fields.

This is deliberately a foundation step: current registration, heartbeat, task, subtask, and scheduler state still use the validated Stage 1–3 in-memory registries. No task/node persistence migration or database migration tooling (Alembic) has been added yet.

## Stage 4B: Persistent node metrics and heartbeat history

Stage 4B wires the existing heartbeat pipeline to PostgreSQL. Every valid heartbeat from a worker now produces one `NodeMetric` row, recording the real `psutil` CPU percentage, memory percentage, and available memory. The `Node` row is also kept in sync (status, hostname, platform, `last_heartbeat`).

The in-memory `NodeRegistry` and `RoundRobinScheduler` remain the authoritative scheduling layer. PostgreSQL is the durable audit log, not the scheduler state.

### Heartbeat persistence flow

```text
Worker (psutil)
    ↓  heartbeat POST
Master in-memory registry updated
    ↓
NodeMetric row inserted (node_id, timestamp, cpu_percent, memory_percent, available_memory)
Node row refreshed (status, last_heartbeat)
    ↓
PostgreSQL
```

A transient database error is logged but does **not** return an error to the worker — heartbeat acceptance is determined by the in-memory registry alone.

### Stage 4B API endpoints

**`GET /api/nodes/<node_id>`**

Returns node identity and live state (from in-memory registry) plus `latest_metric` from PostgreSQL:

```json
{
  "node_id": "node-01",
  "hostname": "abc123",
  "platform": "Linux...",
  "status": "ONLINE",
  "worker_url": "http://node-01:5001",
  "registered_at": "2026-09-23T18:10:00+00:00",
  "last_heartbeat": "2026-09-23T18:15:30+00:00",
  "task_state": "IDLE",
  "cpu_percent": 12.5,
  "memory_percent": 34.0,
  "available_memory": 1073741824,
  "latest_metric": {
    "timestamp": "2026-09-23T18:15:30.123456+00:00",
    "cpu_percent": 12.5,
    "memory_percent": 34.0,
    "available_memory": 1073741824,
    "heartbeat_latency": null
  }
}
```

If no heartbeat has been persisted yet, `latest_metric` is `null`.

**`GET /api/nodes/<node_id>/metrics?limit=N`**

Returns the most recent `N` heartbeat samples for a node, newest-first. Default `limit` is 100.

```json
{
  "node_id": "node-01",
  "count": 3,
  "metrics": [
    {"timestamp": "...", "cpu_percent": 15.0, "memory_percent": 40.0, "available_memory": 900000000, "heartbeat_latency": null},
    {"timestamp": "...", "cpu_percent": 12.0, "memory_percent": 38.5, "available_memory": 920000000, "heartbeat_latency": null},
    {"timestamp": "...", "cpu_percent": 10.0, "memory_percent": 37.0, "available_memory": 940000000, "heartbeat_latency": null}
  ]
}
```

Invalid `limit` values (non-integer, zero, negative) return `400`. Unknown node IDs return `404`.

### `heartbeat_latency`

This field is always `null` in Stage 4B. Reliable latency measurement requires a round-trip timestamp protocol that has not been added to the worker yet. The column exists in the schema for a future stage.

### Offline node behaviour

- When a node misses its heartbeat window, the master marks it `OFFLINE` in memory (unchanged from Stage 1).
- No `NodeMetric` rows are created while a node is offline — there are no fabricated or zero-valued metrics.
- Historical metrics before the outage are preserved.
- When the node recovers (registers and heartbeats again), it becomes `ONLINE` and new `NodeMetric` rows resume.

### Verify with Docker

After the stack is running and nodes have sent several heartbeats:

```powershell
# Node detail with latest metric
curl.exe http://localhost:5000/api/nodes/node-01

# Last 10 heartbeat samples (newest-first)
curl.exe "http://localhost:5000/api/nodes/node-01/metrics?limit=10"
```

### Inspect PostgreSQL directly

```powershell
$db   = (Get-Content .env | Where-Object { $_ -match 'POSTGRES_DB' }).Split('=')[1]
$user = (Get-Content .env | Where-Object { $_ -match 'POSTGRES_USER' }).Split('=')[1]
$id   = docker compose ps -q postgres
docker exec -it $id psql -U $user -d $db -c "SELECT node_id, COUNT(*) FROM node_metrics GROUP BY node_id;"
```

## Test node failure and recovery

1. Stop one actual worker container:

   ```powershell
   docker compose stop node-01
   ```

2. Wait at least 15 seconds (the configured heartbeat timeout), then run:

   ```powershell
   Invoke-RestMethod http://localhost:5000/api/nodes | ConvertTo-Json -Depth 5
   ```

   `node-01` will be `OFFLINE`, while `node-02` and `node-03` stay `ONLINE`. The master log records the transition.

3. Restart it:

   ```powershell
   docker compose start node-01
   ```

   Within one heartbeat interval (five seconds), the agent re-registers and reports as `ONLINE` again.

To confirm Stage 3 avoids an offline worker, stop `node-01`, wait for its `OFFLINE` status, then submit a `sum_range` task. The parent will split only across `ONLINE`, `IDLE` remaining nodes. Stage 3 does not migrate or retry a subtask already assigned to a failed worker.

## Automated tests

Lightweight `unittest` coverage is in `tests/`. The suite covers:

- `test_database.py` — Stage 4A schema, Node model, and connection retry logic.
- `test_stage2.py` — Stage 2 single-node execution regression.
- `test_stage3.py` — Stage 3 range splitting, aggregation, failure, and scheduler tests.
- `test_stage4b.py` — Stage 4B: `NodeMetric` model, heartbeat persistence, multiple records, real metric values, metrics API (basic / limit / ordering), node detail API (basic / `latest_metric`), no duplicate Node rows, offline detection without fake metrics, node recovery, and Stage 2/3 regression.

All tests use an in-memory SQLite database so no running PostgreSQL instance is needed.

With Python dependencies installed locally, run:

```powershell
py -3.14 -m unittest discover -s tests -v
```

## Configuration

The Compose defaults are intentionally conservative and can be changed in `docker-compose.yml`:

- `HEARTBEAT_TIMEOUT_SECONDS=15` on the master.
- `OFFLINE_CHECK_INTERVAL_SECONDS=2` on the master.
- `WORKER_TASK_TIMEOUT_SECONDS=30` and `TASK_DISPATCH_WORKERS=4` on the master (one parent-dispatch thread plus up to three subtasks).
- `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` in `.env`; see `.env.example`.
- `DATABASE_RETRY_ATTEMPTS=30` and `DATABASE_RETRY_DELAY_SECONDS=2` on the master.
- `HEARTBEAT_INTERVAL_SECONDS=5` and `REQUEST_TIMEOUT_SECONDS=3` on each agent.
- `WORKER_URL` and `WORKER_PORT=5001` on each agent. Compose assigns Docker service-name URLs for all three workers.

For production-like deployments, a future stage should put the master behind a suitable server/reverse proxy, add authentication and persistent storage, and make the master URL reachable and secured across machines.
