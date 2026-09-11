# Intelligent Distributive Computing Platform (IDCP)

IDCP is a B.Tech project exploring a predictive, risk-aware, self-healing distributed computing platform. This repository implements Stage 1 distributed infrastructure and **Stage 2 real workload execution** on a single laptop.

It deliberately does not yet include ML, blockchain, a database, React, task splitting, workload migration, predictive/risk-aware scheduling, or advanced scheduling. The Flask master keeps state in memory, and the node agent is isolated so a later stage can replace or extend either concern cleanly.

## What Stages 1 and 2 implement

- A Python/Flask master service with JSON APIs for registration, heartbeats, node listing, and health.
- A reusable Python node agent that collects real `psutil` CPU and memory metrics.
- Three independently running worker containers (`node-01`, `node-02`, and `node-03`).
- In-memory node status tracking and automatic `ONLINE` / `OFFLINE` detection.
- Retry behavior when the master is temporarily unavailable, including recovery after a master restart.
- A real worker-side task API in every node agent, available only inside the Docker network.
- In-memory task lifecycle tracking and asynchronous task dispatch, so task execution does not block heartbeats.
- A simple `ONLINE` + `IDLE` round-robin scheduler.
- One real computation type: `sum_range`.

`available_memory` is reported in bytes, exactly as provided by `psutil`.

## Architecture

```text
Windows laptop (Docker Desktop + WSL2)
            |
       Docker bridge network: idcp-network
            |
  +---------+---------+---------+
  |                   |         |
master             node-01   node-02   node-03
Flask :5000        Node Agent + Task API :5001 (same code in each container)
```

Each agent calls `http://master:5000` through Docker's service-name DNS; it does not use `localhost` or a fixed IP address. The master sends work to the selected agent using its service-name URL, such as `http://node-01:5001/api/tasks/execute`. Only the master is published to the Windows host at `http://localhost:5000`.

The three worker containers are **logical worker nodes**, not three physical computers. They share the host laptop's underlying CPU and RAM, although each runs as an independent process/container and reports the metrics visible within its own container. Later, run the same `node-agent` image or Python program on separate machines/VMs and set `MASTER_URL` to the reachable master address; no change to the agent's collection or heartbeat logic is required.

## Prerequisites

- Windows 10/11 with Docker Desktop installed and running.
- WSL2 backend enabled in Docker Desktop.
- Docker Compose v2 (`docker compose version`).

## Start the platform

From the repository root in PowerShell:

```powershell
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

This stops and removes the containers and Docker network. Stages 1 and 2 have no database or volume to preserve.

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
| `POST` | `/api/nodes/heartbeat` | Submit real node metrics |
| `GET` | `/api/nodes` | List known nodes and live status |
| `GET` | `/api/health` | Check master health and node counts |
| `POST` | `/api/tasks` | Create and asynchronously dispatch a workload |
| `GET` | `/api/tasks` | List all known workloads |
| `GET` | `/api/tasks/<task_id>` | Retrieve one workload and its result |

## Stage 2: real workload execution

Stage 2 adds one meaningful computation:

```json
{
  "task_type": "sum_range",
  "start": 1,
  "end": 1000000
}
```

The selected node performs `sum(range(1, 1000001))` using Python integer arithmetic and returns `500000500000`; the result is not fabricated by the master.

The task lifecycle is:

```text
CREATED -> ASSIGNED -> RUNNING -> COMPLETED
                         |
                         +-> FAILED
```

The POST response always initially reports `CREATED`. A background dispatch thread then selects the next `ONLINE` and `IDLE` node in round-robin order, marks it `BUSY`, sends it the task, stores the real result, and returns that node to `IDLE`. `GET /api/nodes` includes this simple node `task_state` alongside the existing Stage 1 metrics.

Submit and retrieve a workload from PowerShell:

```powershell
$body = @{ task_type = "sum_range"; start = 1; end = 1000000 } | ConvertTo-Json
$created = Invoke-RestMethod -Method Post -Uri http://localhost:5000/api/tasks -ContentType "application/json" -Body $body
$created
Invoke-RestMethod "http://localhost:5000/api/tasks/$($created.task_id)" | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:5000/api/tasks | ConvertTo-Json -Depth 5
```

Submitting several completed tasks in sequence demonstrates round-robin assignment across `node-01`, `node-02`, and `node-03`. Invalid requests are rejected with JSON errors: missing/unsupported `task_type`, non-integer `start` or `end`, and `start > end` are all invalid.

Stage 2 does **not** split a task between nodes, predict workload risk, migrate/checkpoint work, or retry it on a different node. Those are later-stage concerns.

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

To confirm Stage 2 avoids an offline worker, stop `node-01`, wait for its `OFFLINE` status, then submit a `sum_range` task. It will be assigned to an `ONLINE`, `IDLE` node such as `node-02` or `node-03`; Stage 2 does not migrate a task already assigned to a failed worker.

## Automated tests

Lightweight `unittest` coverage is in `tests/test_stage2.py`. It checks the real `sum_range` computation, invalid task rejection, task creation/execution/result retrieval, round-robin assignment, and that an offline node is skipped.

With Python dependencies installed locally, run:

```powershell
python -m unittest tests/test_stage2.py
```

## Configuration

The Compose defaults are intentionally conservative and can be changed in `docker-compose.yml`:

- `HEARTBEAT_TIMEOUT_SECONDS=15` on the master.
- `OFFLINE_CHECK_INTERVAL_SECONDS=2` on the master.
- `WORKER_TASK_TIMEOUT_SECONDS=30` and `TASK_DISPATCH_WORKERS=3` on the master.
- `HEARTBEAT_INTERVAL_SECONDS=5` and `REQUEST_TIMEOUT_SECONDS=3` on each agent.
- `WORKER_URL` and `WORKER_PORT=5001` on each agent. Compose assigns Docker service-name URLs for all three workers.

For production-like deployments, a future stage should put the master behind a suitable server/reverse proxy, add authentication and persistent storage, and make the master URL reachable and secured across machines.
