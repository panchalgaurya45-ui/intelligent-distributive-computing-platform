# Intelligent Distributive Computing Platform (IDCP)

IDCP is a B.Tech second-year project exploring a predictive, risk-aware, self-healing distributed computing platform. This repository implements **Stage 1** only: the real distributed infrastructure on a single laptop.

It deliberately does not yet include ML, blockchain, a database, React, workload migration, or advanced scheduling. The Flask master keeps state in memory, and the node agent is isolated so a later stage can replace or extend either concern cleanly.

## What Stage 1 implements

- A Python/Flask master service with JSON APIs for registration, heartbeats, node listing, and health.
- A reusable Python node agent that collects real `psutil` CPU and memory metrics.
- Three independently running worker containers (`node-01`, `node-02`, and `node-03`).
- In-memory node status tracking and automatic `ONLINE` / `OFFLINE` detection.
- Retry behavior when the master is temporarily unavailable, including recovery after a master restart.

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
Flask :5000        Node Agent (same code in each container)
```

Each agent calls `http://master:5000` through Docker's service-name DNS; it does not use `localhost` or a fixed IP address. The master is published to the Windows host at `http://localhost:5000`.

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

This stops and removes the containers and Docker network. Stage 1 has no database or volume to preserve.

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

## Configuration

The Compose defaults are intentionally conservative and can be changed in `docker-compose.yml`:

- `HEARTBEAT_TIMEOUT_SECONDS=15` on the master.
- `OFFLINE_CHECK_INTERVAL_SECONDS=2` on the master.
- `HEARTBEAT_INTERVAL_SECONDS=5` and `REQUEST_TIMEOUT_SECONDS=3` on each agent.

For production-like deployments, a future stage should put the master behind a suitable server/reverse proxy, add authentication and persistent storage, and make the master URL reachable and secured across machines.
