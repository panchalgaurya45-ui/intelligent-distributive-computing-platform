"""Stage 4B tests: persistent node metrics and heartbeat history.

All tests run against an in-memory SQLite database so they need no running
PostgreSQL instance.  The existing Stage 1–3 behaviour is re-verified through
regression tests at the bottom of this module.
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("IDCP_DATABASE_URI", "sqlite+pysqlite:///:memory:")
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

import app as master  # noqa: E402
from database import db  # noqa: E402
from models import Node, NodeMetric  # noqa: E402


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

def _make_app(**kwargs):
    """Return a fresh test app backed by an isolated in-memory SQLite DB."""
    return master.create_app(
        database_uri="sqlite+pysqlite:///:memory:",
        start_monitor=False,
        **kwargs,
    )


def _register(client, node_id="node-01", hostname="host-01", platform="test-platform",
              worker_url="http://node-01:5001"):
    return client.post(
        "/api/nodes/register",
        json={
            "node_id": node_id,
            "hostname": hostname,
            "platform": platform,
            "worker_url": worker_url,
        },
    )


def _heartbeat(client, node_id="node-01", cpu=10.0, memory=20.0, available=1_000_000_000):
    return client.post(
        "/api/nodes/heartbeat",
        json={
            "node_id": node_id,
            "hostname": "host-01",
            "platform": "test-platform",
            "cpu_percent": cpu,
            "memory_percent": memory,
            "available_memory": available,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


# ---------------------------------------------------------------------------
# 1. NodeMetric table / model existence
# ---------------------------------------------------------------------------

class TestNodeMetricTableExists(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()

    def test_node_metric_table_has_required_columns(self):
        with self.app.app_context():
            cols = {c["name"] for c in inspect(db.engine).get_columns("node_metrics")}
        self.assertTrue(
            {"id", "node_id", "timestamp", "cpu_percent", "memory_percent",
             "available_memory", "heartbeat_latency"}.issubset(cols)
        )

    def test_node_metric_model_can_be_persisted_directly(self):
        """NodeMetric can be inserted when a matching Node row exists."""
        with self.app.app_context():
            node = Node(
                node_id="node-model-test",
                hostname="h",
                platform="p",
                status="ONLINE",
            )
            db.session.add(node)
            db.session.flush()

            ts = datetime.now(timezone.utc)
            metric = NodeMetric(
                node_id="node-model-test",
                timestamp=ts,
                cpu_percent=42.5,
                memory_percent=33.3,
                available_memory=2_000_000_000,
                heartbeat_latency=None,
            )
            db.session.add(metric)
            db.session.commit()

            stored = db.session.get(NodeMetric, metric.id)
            self.assertIsNotNone(stored)
            self.assertEqual(stored.node_id, "node-model-test")
            self.assertAlmostEqual(stored.cpu_percent, 42.5)


# ---------------------------------------------------------------------------
# 2. A heartbeat creates a NodeMetric record
# ---------------------------------------------------------------------------

class TestHeartbeatCreatesMetric(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)

    def test_heartbeat_creates_exactly_one_metric(self):
        resp = _heartbeat(self.client, cpu=55.0, memory=40.0, available=512_000_000)
        self.assertEqual(resp.status_code, 200)

        with self.app.app_context():
            count = db.session.execute(
                db.select(db.func.count()).select_from(NodeMetric)
                .where(NodeMetric.node_id == "node-01")
            ).scalar_one()
        self.assertEqual(count, 1)

    def test_heartbeat_stores_correct_metric_values(self):
        _heartbeat(self.client, cpu=75.5, memory=60.2, available=800_000_000)

        with self.app.app_context():
            row = db.session.execute(
                db.select(NodeMetric).where(NodeMetric.node_id == "node-01")
            ).scalar_one()
        self.assertAlmostEqual(row.cpu_percent, 75.5, places=4)
        self.assertAlmostEqual(row.memory_percent, 60.2, places=4)
        self.assertEqual(row.available_memory, 800_000_000)


# ---------------------------------------------------------------------------
# 3. Multiple heartbeats create multiple records
# ---------------------------------------------------------------------------

class TestMultipleHeartbeatsCreateMultipleRecords(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)

    def test_three_heartbeats_produce_three_rows(self):
        for cpu in (10.0, 20.0, 30.0):
            resp = _heartbeat(self.client, cpu=cpu)
            self.assertEqual(resp.status_code, 200)

        with self.app.app_context():
            count = db.session.execute(
                db.select(db.func.count()).select_from(NodeMetric)
                .where(NodeMetric.node_id == "node-01")
            ).scalar_one()
        self.assertEqual(count, 3)


# ---------------------------------------------------------------------------
# 4. Metrics contain real field values (not zeros / None)
# ---------------------------------------------------------------------------

class TestMetricValuesAreReal(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)

    def test_metric_fields_are_non_trivial(self):
        _heartbeat(self.client, cpu=18.7, memory=55.1, available=1_234_567_890)

        with self.app.app_context():
            row = db.session.execute(
                db.select(NodeMetric).where(NodeMetric.node_id == "node-01")
            ).scalar_one()

        self.assertGreater(row.cpu_percent, 0)
        self.assertGreater(row.memory_percent, 0)
        self.assertGreater(row.available_memory, 0)
        self.assertIsNotNone(row.cpu_percent)
        self.assertIsNotNone(row.memory_percent)
        self.assertIsNotNone(row.available_memory)

    def test_heartbeat_latency_is_null_not_invented(self):
        """heartbeat_latency must be null — we must not invent fake values."""
        _heartbeat(self.client)
        with self.app.app_context():
            row = db.session.execute(
                db.select(NodeMetric).where(NodeMetric.node_id == "node-01")
            ).scalar_one()
        self.assertIsNone(row.heartbeat_latency)


# ---------------------------------------------------------------------------
# 5. Metrics API — basic operation
# ---------------------------------------------------------------------------

class TestMetricsApiBasic(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)
        _heartbeat(self.client, cpu=22.0, memory=33.0, available=500_000_000)

    def test_metrics_endpoint_returns_200(self):
        resp = self.client.get("/api/nodes/node-01/metrics")
        self.assertEqual(resp.status_code, 200)

    def test_metrics_response_contains_expected_keys(self):
        data = self.client.get("/api/nodes/node-01/metrics").get_json()
        self.assertIn("node_id", data)
        self.assertIn("count", data)
        self.assertIn("metrics", data)
        self.assertEqual(data["node_id"], "node-01")

    def test_metrics_entry_has_all_fields(self):
        data = self.client.get("/api/nodes/node-01/metrics").get_json()
        self.assertEqual(data["count"], 1)
        entry = data["metrics"][0]
        for field in ("timestamp", "cpu_percent", "memory_percent", "available_memory", "heartbeat_latency"):
            self.assertIn(field, entry)

    def test_unknown_node_returns_404(self):
        resp = self.client.get("/api/nodes/does-not-exist/metrics")
        self.assertEqual(resp.status_code, 404)


# ---------------------------------------------------------------------------
# 6. Metrics API — ?limit parameter
# ---------------------------------------------------------------------------

class TestMetricsApiLimit(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)
        for i in range(5):
            _heartbeat(self.client, cpu=float(i + 1))

    def test_default_limit_returns_all_when_fewer_than_100(self):
        data = self.client.get("/api/nodes/node-01/metrics").get_json()
        self.assertEqual(data["count"], 5)

    def test_limit_parameter_is_respected(self):
        data = self.client.get("/api/nodes/node-01/metrics?limit=3").get_json()
        self.assertEqual(data["count"], 3)
        self.assertEqual(len(data["metrics"]), 3)

    def test_limit_of_one_returns_one_metric(self):
        data = self.client.get("/api/nodes/node-01/metrics?limit=1").get_json()
        self.assertEqual(data["count"], 1)

    def test_invalid_limit_returns_400(self):
        for bad in ("abc", "0", "-5", "1.5"):
            with self.subTest(limit=bad):
                resp = self.client.get(f"/api/nodes/node-01/metrics?limit={bad}")
                self.assertEqual(resp.status_code, 400)
                self.assertIn("error", resp.get_json())


# ---------------------------------------------------------------------------
# 7. Metrics API — ordering (newest-first)
# ---------------------------------------------------------------------------

class TestMetricsApiOrdering(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)

    def test_metrics_returned_newest_first(self):
        # Insert metrics with distinct cpu_percent values in order.
        for cpu in (1.0, 2.0, 3.0):
            _heartbeat(self.client, cpu=cpu)
            time.sleep(0.01)  # Ensure strictly increasing timestamps.

        data = self.client.get("/api/nodes/node-01/metrics").get_json()
        cpu_values = [m["cpu_percent"] for m in data["metrics"]]
        # Newest first → descending cpu (3.0, 2.0, 1.0).
        self.assertEqual(cpu_values, sorted(cpu_values, reverse=True))

    def test_limit_with_ordering_returns_most_recent(self):
        for cpu in (5.0, 10.0, 15.0):
            _heartbeat(self.client, cpu=cpu)
            time.sleep(0.01)

        data = self.client.get("/api/nodes/node-01/metrics?limit=1").get_json()
        # With newest-first, the single result should be the last sent (15.0).
        self.assertAlmostEqual(data["metrics"][0]["cpu_percent"], 15.0, places=4)


# ---------------------------------------------------------------------------
# 8. Node detail API — basic operation
# ---------------------------------------------------------------------------

class TestNodeDetailApiBasic(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)

    def test_node_detail_returns_200(self):
        resp = self.client.get("/api/nodes/node-01")
        self.assertEqual(resp.status_code, 200)

    def test_node_detail_contains_standard_fields(self):
        data = self.client.get("/api/nodes/node-01").get_json()
        for field in ("node_id", "hostname", "platform", "status", "worker_url",
                      "registered_at", "last_heartbeat"):
            self.assertIn(field, data)

    def test_node_detail_contains_latest_metric_key(self):
        data = self.client.get("/api/nodes/node-01").get_json()
        self.assertIn("latest_metric", data)

    def test_unknown_node_returns_404(self):
        resp = self.client.get("/api/nodes/ghost-node")
        self.assertEqual(resp.status_code, 404)


# ---------------------------------------------------------------------------
# 9. Node detail API — latest_metric value
# ---------------------------------------------------------------------------

class TestNodeDetailLatestMetric(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()
        _register(self.client)

    def test_latest_metric_is_null_before_any_heartbeat(self):
        data = self.client.get("/api/nodes/node-01").get_json()
        self.assertIsNone(data["latest_metric"])

    def test_latest_metric_populated_after_heartbeat(self):
        _heartbeat(self.client, cpu=88.8, memory=44.4, available=999_000_000)
        data = self.client.get("/api/nodes/node-01").get_json()
        self.assertIsNotNone(data["latest_metric"])
        m = data["latest_metric"]
        self.assertAlmostEqual(m["cpu_percent"], 88.8, places=4)
        self.assertAlmostEqual(m["memory_percent"], 44.4, places=4)
        self.assertEqual(m["available_memory"], 999_000_000)

    def test_latest_metric_reflects_most_recent_heartbeat(self):
        _heartbeat(self.client, cpu=10.0)
        time.sleep(0.01)
        _heartbeat(self.client, cpu=99.9)

        data = self.client.get("/api/nodes/node-01").get_json()
        self.assertAlmostEqual(data["latest_metric"]["cpu_percent"], 99.9, places=4)


# ---------------------------------------------------------------------------
# 10. Registration does not create duplicate Node rows
# ---------------------------------------------------------------------------

class TestRegistrationNoDuplicates(unittest.TestCase):
    def setUp(self):
        self.app = _make_app()
        self.client = self.app.test_client()

    def test_repeated_registration_does_not_duplicate_db_row(self):
        for _ in range(3):
            resp = _register(self.client)
            self.assertIn(resp.status_code, (200, 201))

        with self.app.app_context():
            count = db.session.execute(
                db.select(db.func.count()).select_from(Node)
                .where(Node.node_id == "node-01")
            ).scalar_one()
        self.assertEqual(count, 1)

    def test_re_registration_updates_existing_row(self):
        _register(self.client, hostname="old-host")
        _register(self.client, hostname="new-host")

        with self.app.app_context():
            row = db.session.execute(
                db.select(Node).where(Node.node_id == "node-01")
            ).scalar_one()
        self.assertEqual(row.hostname, "new-host")


# ---------------------------------------------------------------------------
# 11. Offline detection — no fake metrics created while offline
# ---------------------------------------------------------------------------

class TestOfflineDetectionNoFakeMetrics(unittest.TestCase):
    def setUp(self):
        self.app = _make_app(heartbeat_timeout_seconds=0.05)
        self.client = self.app.test_client()
        _register(self.client)
        _heartbeat(self.client)

    def test_node_goes_offline_after_timeout(self):
        time.sleep(0.1)
        # Trigger the check by calling list_nodes (which calls mark_timed_out_nodes_offline).
        data = self.client.get("/api/nodes").get_json()
        statuses = {n["node_id"]: n["status"] for n in data["nodes"]}
        self.assertEqual(statuses["node-01"], "OFFLINE")

    def test_no_additional_metrics_created_while_offline(self):
        with self.app.app_context():
            before = db.session.execute(
                db.select(db.func.count()).select_from(NodeMetric)
                .where(NodeMetric.node_id == "node-01")
            ).scalar_one()

        time.sleep(0.1)
        # Trigger offline detection.
        self.client.get("/api/nodes")
        # Wait more time to confirm no background writes happened.
        time.sleep(0.1)

        with self.app.app_context():
            after = db.session.execute(
                db.select(db.func.count()).select_from(NodeMetric)
                .where(NodeMetric.node_id == "node-01")
            ).scalar_one()

        self.assertEqual(before, after)


# ---------------------------------------------------------------------------
# 12. Returning node becomes ONLINE again
# ---------------------------------------------------------------------------

class TestNodeRecovery(unittest.TestCase):
    def setUp(self):
        self.app = _make_app(heartbeat_timeout_seconds=0.05)
        self.client = self.app.test_client()
        _register(self.client)
        _heartbeat(self.client)

    def test_node_becomes_online_again_after_heartbeat(self):
        time.sleep(0.1)
        # Node is now OFFLINE.
        data_offline = self.client.get("/api/nodes").get_json()
        self.assertEqual(
            next(n["status"] for n in data_offline["nodes"] if n["node_id"] == "node-01"),
            "OFFLINE",
        )

        # Simulate recovery: re-register then heartbeat.
        _register(self.client)
        _heartbeat(self.client, cpu=5.0)

        data_online = self.client.get("/api/nodes").get_json()
        self.assertEqual(
            next(n["status"] for n in data_online["nodes"] if n["node_id"] == "node-01"),
            "ONLINE",
        )

    def test_new_metrics_resume_after_recovery(self):
        time.sleep(0.1)
        _register(self.client)
        _heartbeat(self.client, cpu=77.0)

        with self.app.app_context():
            count = db.session.execute(
                db.select(db.func.count()).select_from(NodeMetric)
                .where(NodeMetric.node_id == "node-01")
            ).scalar_one()
        # At least 2 rows: 1 before going offline, 1 after recovery.
        self.assertGreaterEqual(count, 2)


# ---------------------------------------------------------------------------
# 13. Stage 2 behaviour regression
# ---------------------------------------------------------------------------

class ImmediateExecutor:
    def submit(self, function, /, *args, **kwargs):
        function(*args, **kwargs)


class RecordingWorkerClient:
    def __init__(self):
        self.payloads = []

    def execute(self, worker_url, payload):
        self.payloads.append(payload)
        p = payload["parameters"]
        return sum(range(p["start"], p["end"] + 1))


class TestStage2Regression(unittest.TestCase):
    def setUp(self):
        self.worker = RecordingWorkerClient()
        app = _make_app(task_executor=ImmediateExecutor(), worker_client=self.worker)
        self.client = app.test_client()
        resp = _register(self.client)
        self.assertEqual(resp.status_code, 201)

    def test_single_node_sum_range_completes(self):
        resp = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 10})
        self.assertEqual(resp.status_code, 201)
        task_id = resp.get_json()["task_id"]
        task = self.client.get(f"/api/tasks/{task_id}").get_json()
        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["result"], 55)
        self.assertEqual(task["final_result"], 55)

    def test_invalid_range_is_rejected(self):
        resp = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 9, "end": 1})
        self.assertEqual(resp.status_code, 400)


# ---------------------------------------------------------------------------
# 14. Stage 3 behaviour regression
# ---------------------------------------------------------------------------

class TestStage3Regression(unittest.TestCase):
    def setUp(self):
        self.worker = RecordingWorkerClient()
        self.app = _make_app(task_executor=ImmediateExecutor(), worker_client=self.worker)
        self.client = self.app.test_client()
        for i in range(1, 4):
            node_id = f"node-{i:02d}"
            resp = self.client.post(
                "/api/nodes/register",
                json={
                    "node_id": node_id,
                    "hostname": f"{node_id}-host",
                    "platform": "test-platform",
                    "worker_url": f"http://{node_id}:5001",
                },
            )
            self.assertEqual(resp.status_code, 201)

    def test_three_workers_aggregate_correctly(self):
        resp = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 1_000_000})
        self.assertEqual(resp.status_code, 201)
        task = self.client.get(f"/api/tasks/{resp.get_json()['task_id']}").get_json()
        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["total_subtasks"], 3)
        self.assertEqual(task["final_result"], 500_000_500_000)

    def test_nodes_return_to_idle_after_task(self):
        resp = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 6})
        self.client.get(f"/api/tasks/{resp.get_json()['task_id']}")
        nodes = self.client.get("/api/nodes").get_json()["nodes"]
        self.assertTrue(all(n["task_state"] == "IDLE" for n in nodes))


if __name__ == "__main__":
    unittest.main()
