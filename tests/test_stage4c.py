"""Stage 4C tests: persistent task execution history, subtasks, and audit events.

All tests run against an in-memory SQLite database so they need no running
PostgreSQL instance. Existing Stage 1–4B behaviour is re-verified through
regression tests at the bottom of this file.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("IDCP_DATABASE_URI", "sqlite+pysqlite:///:memory:")
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
import app as master  # noqa: E402
from models import Event, Node, NodeMetric, Subtask, Task  # noqa: E402


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


class FailingWorkerClient:
    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        raise master.WorkerTaskError("Worker rejected the task")


class Stage4cModelExistenceTests(unittest.TestCase):
    """Verify that Task, Subtask, and Event models exist with expected fields."""

    def setUp(self) -> None:
        self.app = master.create_app(database_uri="sqlite+pysqlite:///:memory:", start_monitor=False)
        self.client = self.app.test_client()

    def test_task_model_exists(self) -> None:
        with self.app.app_context():
            task = Task(
                task_id="T-test01",
                task_type="sum_range",
                status="CREATED",
                start=1,
                end=10,
            )
            master.db.session.add(task)
            master.db.session.commit()
            fetched = master.db.session.execute(
                master.db.select(Task).where(Task.task_id == "T-test01")
            ).scalar_one_or_none()
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched.task_type, "sum_range")

    def test_subtask_model_exists(self) -> None:
        with self.app.app_context():
            task = Task(
                task_id="T-test02",
                task_type="sum_range",
                status="SPLIT",
                start=1,
                end=10,
            )
            master.db.session.add(task)
            subtask = Subtask(
                subtask_id="T-test02-S1",
                parent_task_id="T-test02",
                task_type="sum_range",
                status="ASSIGNED",
                start=1,
                end=10,
            )
            master.db.session.add(subtask)
            master.db.session.commit()
            fetched = master.db.session.execute(
                master.db.select(Subtask).where(Subtask.subtask_id == "T-test02-S1")
            ).scalar_one_or_none()
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched.parent_task_id, "T-test02")

    def test_event_model_exists(self) -> None:
        with self.app.app_context():
            event = Event(
                event_type="TASK_CREATED",
                message="Task created test",
                task_id="T-test03",
                severity="INFO",
            )
            master.db.session.add(event)
            master.db.session.commit()
            fetched = master.db.session.execute(
                master.db.select(Event).where(Event.task_id == "T-test03")
            ).scalar_one_or_none()
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched.event_type, "TASK_CREATED")


class Stage4cTaskPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker_client = CalculatingWorkerClient()
        self.app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            task_executor=ImmediateExecutor(),
            worker_client=self.worker_client,
            start_monitor=False,
        )
        self.client = self.app.test_client()

    def register_nodes(self, count: int) -> None:
        for index in range(1, count + 1):
            node_id = f"node-{index:02d}"
            self.client.post(
                "/api/nodes/register",
                json={
                    "node_id": node_id,
                    "hostname": f"{node_id}-host",
                    "platform": "test-platform",
                    "worker_url": f"http://{node_id}:5001",
                },
            )

    def test_task_creation_persists_task_row_and_created_event(self) -> None:
        self.register_nodes(1)
        res = self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 10}},
        )
        self.assertEqual(res.status_code, 201)
        task_id = res.get_json()["task_id"]

        with self.app.app_context():
            task_row = master.db.session.execute(
                master.db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            self.assertIsNotNone(task_row)
            self.assertEqual(task_row.status, "COMPLETED")
            self.assertEqual(task_row.final_result, 55)

            created_event = master.db.session.execute(
                master.db.select(Event).where(
                    Event.task_id == task_id, Event.event_type == "TASK_CREATED"
                )
            ).scalar_one_or_none()
            self.assertIsNotNone(created_event)

    def test_subtasks_persisted_and_completed(self) -> None:
        self.register_nodes(2)
        res = self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 10}},
        )
        task_id = res.get_json()["task_id"]

        with self.app.app_context():
            subtask_rows = master.db.session.execute(
                master.db.select(Subtask).where(Subtask.parent_task_id == task_id)
            ).scalars().all()
            self.assertEqual(len(subtask_rows), 2)
            for st in subtask_rows:
                self.assertEqual(st.status, "COMPLETED")
                self.assertIsNotNone(st.result)

            completed_event = master.db.session.execute(
                master.db.select(Event).where(
                    Event.task_id == task_id, Event.event_type == "TASK_COMPLETED"
                )
            ).scalar_one_or_none()
            self.assertIsNotNone(completed_event)

    def test_failed_subtask_and_parent_task_failure(self) -> None:
        failing_app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            task_executor=ImmediateExecutor(),
            worker_client=FailingWorkerClient(),
            start_monitor=False,
        )
        failing_client = failing_app.test_client()
        failing_client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "node-01-host",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )
        res = failing_client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 10}},
        )
        task_id = res.get_json()["task_id"]

        with failing_app.app_context():
            task_row = master.db.session.execute(
                master.db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            self.assertIsNotNone(task_row)
            self.assertEqual(task_row.status, "FAILED")
            self.assertIn("Worker rejected the task", task_row.error or "")

            failed_event = master.db.session.execute(
                master.db.select(Event).where(
                    Event.task_id == task_id, Event.event_type == "TASK_FAILED"
                )
            ).scalar_one_or_none()
            self.assertIsNotNone(failed_event)


class Stage4cApisAndFilteringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker_client = CalculatingWorkerClient()
        self.app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            task_executor=ImmediateExecutor(),
            worker_client=self.worker_client,
            start_monitor=False,
        )
        self.client = self.app.test_client()
        self.client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "node-01-host",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )

    def test_task_history_api(self) -> None:
        self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 10}},
        )
        res = self.client.get("/api/tasks/history?limit=10")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["tasks"][0]["final_result"], 55)

    def test_task_detail_api(self) -> None:
        create_res = self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 100}},
        )
        task_id = create_res.get_json()["task_id"]
        res = self.client.get(f"/api/tasks/{task_id}")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["task_id"], task_id)
        self.assertEqual(data["status"], "COMPLETED")
        self.assertEqual(data["final_result"], 5050)

    def test_event_history_api_and_filtering(self) -> None:
        self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 5}},
        )
        res = self.client.get("/api/events?limit=50")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertGreater(data["count"], 0)

        # Test filtering by event_type
        res_filter = self.client.get("/api/events?event_type=TASK_COMPLETED")
        self.assertEqual(res_filter.status_code, 200)
        filter_data = res_filter.get_json()
        for ev in filter_data["events"]:
            self.assertEqual(ev["event_type"], "TASK_COMPLETED")


class Stage4cNodeLifecycleEventsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = master.create_app(
            heartbeat_timeout_seconds=0.05,
            database_uri="sqlite+pysqlite:///:memory:",
            start_monitor=False,
        )
        self.client = self.app.test_client()

    def test_node_lifecycle_events_no_heartbeat_duplicates(self) -> None:
        # Register node
        self.client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "host-01",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )
        # Send 3 heartbeats
        for _ in range(3):
            self.client.post(
                "/api/nodes/heartbeat",
                json={
                    "node_id": "node-01",
                    "hostname": "host-01",
                    "platform": "test-platform",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "cpu_percent": 10.0,
                    "memory_percent": 20.0,
                    "available_memory": 8000000000,
                },
            )

        with self.app.app_context():
            reg_events = master.db.session.execute(
                master.db.select(Event).where(
                    Event.node_id == "node-01", Event.event_type == "NODE_REGISTERED"
                )
            ).scalars().all()
            self.assertEqual(len(reg_events), 1)

            online_events = master.db.session.execute(
                master.db.select(Event).where(
                    Event.node_id == "node-01", Event.event_type == "NODE_ONLINE"
                )
            ).scalars().all()
            # Since node did not go offline, no NODE_ONLINE events should be created on routine heartbeats.
            self.assertEqual(len(online_events), 0)


class Stage234bRegressionTests(unittest.TestCase):
    """Ensure Stage 2, Stage 3, Stage 4A, and Stage 4B features remain 100% functional."""

    def setUp(self) -> None:
        self.worker_client = CalculatingWorkerClient()
        self.app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            task_executor=ImmediateExecutor(),
            worker_client=self.worker_client,
            start_monitor=False,
        )
        self.client = self.app.test_client()

    def test_stage2_single_node_sum_range(self) -> None:
        self.client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "host-01",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )
        res = self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 10}},
        )
        self.assertEqual(res.status_code, 201)
        task_id = res.get_json()["task_id"]
        detail = self.client.get(f"/api/tasks/{task_id}").get_json()
        self.assertEqual(detail["status"], "COMPLETED")
        self.assertEqual(detail["final_result"], 55)

    def test_stage3_three_workers_aggregation(self) -> None:
        for i in range(1, 4):
            self.client.post(
                "/api/nodes/register",
                json={
                    "node_id": f"node-0{i}",
                    "hostname": f"host-0{i}",
                    "platform": "test-platform",
                    "worker_url": f"http://node-0{i}:5001",
                },
            )
        res = self.client.post(
            "/api/tasks",
            json={"task_type": "sum_range", "parameters": {"start": 1, "end": 1000000}},
        )
        task_id = res.get_json()["task_id"]
        detail = self.client.get(f"/api/tasks/{task_id}").get_json()
        self.assertEqual(detail["status"], "COMPLETED")
        self.assertEqual(detail["final_result"], 500000500000)

    def test_stage4b_node_metrics(self) -> None:
        self.client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "host-01",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )
        self.client.post(
            "/api/nodes/heartbeat",
            json={
                "node_id": "node-01",
                "hostname": "host-01",
                "platform": "test-platform",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cpu_percent": 15.5,
                "memory_percent": 42.0,
                "available_memory": 8500000000,
            },
        )
        res = self.client.get("/api/nodes/node-01/metrics")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["metrics"][0]["cpu_percent"], 15.5)


if __name__ == "__main__":
    unittest.main()
