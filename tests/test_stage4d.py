"""Unit tests for Stage 4D — Monitoring APIs, Hardened Queries, and Integration Verification.

Tests cover:
- GET /api/monitoring/summary endpoint (nodes, cluster metrics, tasks, events summary)
- Limit capping for node metrics, task history, and event history
- Query optimization (joinedload for subtasks)
- Preserving existing Stage 1-4C functionalities (nodes, tasks, persistence, events)
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("IDCP_DATABASE_URI", "sqlite+pysqlite:///:memory:")
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

import app as master  # noqa: E402
from database import db  # noqa: E402
from models import Event, NodeMetric, Subtask, Task  # noqa: E402


def load_agent_module() -> Any:
    try:
        import psutil  # noqa: F401
    except ModuleNotFoundError:
        sys.modules["psutil"] = types.ModuleType("psutil")
    spec = importlib.util.spec_from_file_location("idcp_node_agent", PROJECT_ROOT / "node-agent" / "agent.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load node-agent module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


agent = load_agent_module()


class ImmediateExecutor:
    def submit(self, function: Any, /, *args: Any, **kwargs: Any) -> None:
        function(*args, **kwargs)


class CalculatingWorkerClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        self.calls.append((worker_url, payload))
        parameters = payload["parameters"]
        return agent.sum_range(parameters["start"], parameters["end"])


def _register(client, node_id="node-01", hostname="host-01", platform="linux", total_memory=16000000000, worker_url="http://node-01:5001"):
    return client.post(
        "/api/nodes/register",
        json={
            "node_id": node_id,
            "hostname": hostname,
            "platform": platform,
            "total_memory": total_memory,
            "worker_url": worker_url,
        },
    )


def _heartbeat(client, node_id="node-01", hostname="host-01", platform="linux", cpu=10.0, memory=20.0, available=1000000000):
    return client.post(
        "/api/nodes/heartbeat",
        json={
            "node_id": node_id,
            "hostname": hostname,
            "platform": platform,
            "cpu_percent": cpu,
            "memory_percent": memory,
            "available_memory": available,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


class Stage4dMonitoringSummaryTests(unittest.TestCase):
    """Test suite for GET /api/monitoring/summary and monitoring counts."""

    def setUp(self) -> None:
        self.app = master.create_app(database_uri="sqlite+pysqlite:///:memory:", start_monitor=False)
        self.client = self.app.test_client()

    def test_monitoring_summary_empty_state(self) -> None:
        """Verify summary response when database and registry are empty."""
        response = self.client.get("/api/monitoring/summary")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()

        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["service"], "idcp-master")
        self.assertIn("timestamp", data)

        # Node counts
        self.assertEqual(data["nodes"]["total_nodes"], 0)
        self.assertEqual(data["nodes"]["online_nodes"], 0)
        self.assertEqual(data["nodes"]["offline_nodes"], 0)
        self.assertEqual(data["nodes"]["busy_nodes"], 0)
        self.assertEqual(data["nodes"]["idle_nodes"], 0)

        # Cluster metrics
        self.assertIsNone(data["cluster_metrics"]["average_cpu_percent"])
        self.assertIsNone(data["cluster_metrics"]["average_memory_percent"])
        self.assertIsNone(data["cluster_metrics"]["total_available_memory"])
        self.assertEqual(data["cluster_metrics"]["online_nodes_included"], 0)

        # Task counts
        self.assertEqual(data["tasks"]["total_tasks"], 0)
        self.assertEqual(data["tasks"]["active_tasks"], 0)
        self.assertEqual(data["tasks"]["completed_tasks"], 0)
        self.assertEqual(data["tasks"]["failed_tasks"], 0)

        # Event counts
        self.assertEqual(data["events"]["total_events"], 0)
        self.assertEqual(data["events"]["recent_events_count_24h"], 0)

    def test_monitoring_summary_node_counts_and_cluster_metrics(self) -> None:
        """Verify node state counts and cluster metrics derived strictly from ONLINE nodes."""
        _register(self.client, node_id="node-01", hostname="host-01", platform="linux", total_memory=16000000000)
        _heartbeat(self.client, node_id="node-01", hostname="host-01", platform="linux", cpu=20.0, memory=50.0, available=8000000000)

        _register(self.client, node_id="node-02", hostname="host-02", platform="linux", total_memory=32000000000)
        _heartbeat(self.client, node_id="node-02", hostname="host-02", platform="linux", cpu=40.0, memory=70.0, available=9600000000)

        # Set runtime node state to BUSY for node-02
        nodes_registry = self.app.config["NODE_REGISTRY"]
        nodes_registry.set_task_state("node-02", "BUSY")

        response = self.client.get("/api/monitoring/summary")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()

        nodes_summary = data["nodes"]
        self.assertEqual(nodes_summary["total_nodes"], 2)
        self.assertEqual(nodes_summary["online_nodes"], 2)
        self.assertEqual(nodes_summary["offline_nodes"], 0)
        self.assertEqual(nodes_summary["busy_nodes"], 1)
        self.assertEqual(nodes_summary["idle_nodes"], 1)

        cluster_metrics = data["cluster_metrics"]
        self.assertEqual(cluster_metrics["average_cpu_percent"], 30.0)
        self.assertEqual(cluster_metrics["average_memory_percent"], 60.0)
        self.assertEqual(cluster_metrics["total_available_memory"], 17600000000)
        self.assertEqual(cluster_metrics["online_nodes_included"], 2)

    def test_monitoring_summary_offline_nodes_exclusion(self) -> None:
        """Verify OFFLINE nodes are excluded from cluster metric averages."""
        _register(self.client, node_id="online-1", hostname="h1")
        _heartbeat(self.client, node_id="online-1", hostname="h1", cpu=10.0, memory=20.0, available=1000)

        _register(self.client, node_id="offline-1", hostname="h2")
        _heartbeat(self.client, node_id="offline-1", hostname="h2", cpu=90.0, memory=90.0, available=5000)

        nodes_registry = self.app.config["NODE_REGISTRY"]
        nodes_registry._nodes["offline-1"]["last_seen"] = time.time() - 100
        nodes_registry._nodes["offline-1"]["status"] = "OFFLINE"

        response = self.client.get("/api/monitoring/summary")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()

        self.assertEqual(data["nodes"]["total_nodes"], 2)
        self.assertEqual(data["nodes"]["online_nodes"], 1)
        self.assertEqual(data["nodes"]["offline_nodes"], 1)

        self.assertEqual(data["cluster_metrics"]["average_cpu_percent"], 10.0)
        self.assertEqual(data["cluster_metrics"]["average_memory_percent"], 20.0)
        self.assertEqual(data["cluster_metrics"]["total_available_memory"], 1000)
        self.assertEqual(data["cluster_metrics"]["online_nodes_included"], 1)

    def test_monitoring_summary_task_and_event_counts(self) -> None:
        """Verify task status counts and event history metrics."""
        with self.app.app_context():
            t1 = Task(task_id="T-01", task_type="sum_range", start=1, end=10, status="COMPLETED")
            t2 = Task(task_id="T-02", task_type="sum_range", start=1, end=10, status="FAILED")
            t3 = Task(task_id="T-03", task_type="sum_range", start=1, end=10, status="CREATED")
            db.session.add_all([t1, t2, t3])

            e1 = Event(event_type="TASK_CREATED", task_id="T-01", message="Task created")
            e2 = Event(event_type="TASK_COMPLETED", task_id="T-01", message="Task completed")
            e3 = Event(event_type="TASK_FAILED", task_id="T-02", message="Task failed")
            db.session.add_all([e1, e2, e3])
            db.session.commit()

        response = self.client.get("/api/monitoring/summary")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()

        self.assertEqual(data["tasks"]["total_tasks"], 3)
        self.assertEqual(data["tasks"]["completed_tasks"], 1)
        self.assertEqual(data["tasks"]["failed_tasks"], 1)
        self.assertEqual(data["events"]["total_events"], 3)
        self.assertEqual(data["events"]["recent_events_count_24h"], 3)


class Stage4dQueryLimitsAndOptimizationsTests(unittest.TestCase):
    """Test suite for limit capping and query optimization on endpoints."""

    def setUp(self) -> None:
        self.app = master.create_app(database_uri="sqlite+pysqlite:///:memory:", start_monitor=False)
        self.client = self.app.test_client()

    def test_node_metrics_limit_capping(self) -> None:
        """Verify node metrics endpoint limits are respected and capped at 1000."""
        _register(self.client, node_id="node-cap", hostname="host-cap")

        with self.app.app_context():
            for i in range(15):
                db.session.add(NodeMetric(
                    node_id="node-cap",
                    timestamp=datetime.now(timezone.utc),
                    cpu_percent=float(i),
                    memory_percent=10.0,
                    available_memory=1000,
                ))
            db.session.commit()

        resp = self.client.get("/api/nodes/node-cap/metrics")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["count"], 15)

        resp = self.client.get("/api/nodes/node-cap/metrics?limit=5")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["count"], 5)

        resp = self.client.get("/api/nodes/node-cap/metrics?limit=5000")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["count"], 15)

        resp = self.client.get("/api/nodes/node-cap/metrics?limit=invalid")
        self.assertEqual(resp.status_code, 400)

    def test_task_history_limit_and_subtask_eager_loading(self) -> None:
        """Verify task history limit capping and subtasks eagerly loaded."""
        _register(self.client, node_id="node-1", hostname="h1")

        with self.app.app_context():
            for i in range(10):
                task = Task(task_id=f"T-cap-{i}", task_type="sum_range", status="COMPLETED")
                subtask = Subtask(subtask_id=f"T-cap-{i}-S1", parent_task_id=f"T-cap-{i}", node_id="node-1", task_type="sum_range", status="COMPLETED")
                db.session.add_all([task, subtask])
            db.session.commit()

        resp = self.client.get("/api/tasks/history?limit=3")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["count"], 3)
        self.assertEqual(len(data["tasks"]), 3)
        self.assertEqual(len(data["tasks"][0]["subtasks"]), 1)
        self.assertEqual(data["tasks"][0]["subtasks"][0]["subtask_id"], f"{data['tasks'][0]['task_id']}-S1")

        resp = self.client.get("/api/tasks/history?limit=9999")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["count"], 10)

    def test_event_history_limit_capping(self) -> None:
        """Verify event history limit capping and filters."""
        with self.app.app_context():
            for i in range(10):
                db.session.add(Event(event_type="NODE_REGISTERED", node_id=f"node-{i}", message=f"Registered node-{i}"))
            db.session.commit()

        resp = self.client.get("/api/events?limit=4")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["count"], 4)

        resp = self.client.get("/api/events?limit=1000")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["count"], 10)


class Stage4dRegressionTests(unittest.TestCase):
    """Regression test suite for Stage 1-4C features."""

    def setUp(self) -> None:
        self.app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            start_monitor=False,
            task_executor=ImmediateExecutor(),
            worker_client=CalculatingWorkerClient(),
        )
        self.client = self.app.test_client()

    def test_full_task_execution_flow(self) -> None:
        """Verify registration, heartbeat, task creation, execution, persistence, and summary integration."""
        reg_resp = _register(self.client, node_id="worker-1", hostname="host-w1", platform="win32", worker_url="http://worker-1:5001")
        self.assertEqual(reg_resp.status_code, 201)

        hb_resp = _heartbeat(self.client, node_id="worker-1", hostname="host-w1", platform="win32", cpu=15.0, memory=40.0, available=8000000000)
        self.assertEqual(hb_resp.status_code, 200)

        task_resp = self.client.post("/api/tasks", json={
            "task_type": "sum_range",
            "parameters": {"start": 1, "end": 100},
        })
        self.assertEqual(task_resp.status_code, 201)
        task_id = task_resp.get_json()["task_id"]

        detail_resp = self.client.get(f"/api/tasks/{task_id}")
        self.assertEqual(detail_resp.status_code, 200)
        self.assertEqual(detail_resp.get_json()["result"], 5050)

        summary_resp = self.client.get("/api/monitoring/summary")
        self.assertEqual(summary_resp.status_code, 200)
        s_data = summary_resp.get_json()
        self.assertEqual(s_data["nodes"]["online_nodes"], 1)
        self.assertEqual(s_data["tasks"]["completed_tasks"], 1)
        self.assertGreaterEqual(s_data["events"]["total_events"], 3)


if __name__ == "__main__":
    unittest.main()
