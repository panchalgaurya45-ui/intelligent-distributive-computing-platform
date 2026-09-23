"""Focused regression tests for Stage 2's one-node execution contract.

Stage 3 extends a task into subtasks, but a single available worker must retain
the observable Stage 2 behavior: one range is executed and ``result`` is set.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("IDCP_DATABASE_URI", "sqlite+pysqlite:///:memory:")
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
import app as master  # noqa: E402


class ImmediateExecutor:
    def submit(self, function: Any, /, *args: Any, **kwargs: Any) -> None:
        function(*args, **kwargs)


class RecordingWorkerClient:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        self.payloads.append(payload)
        parameters = payload["parameters"]
        return sum(range(parameters["start"], parameters["end"] + 1))


class Stage2CompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = RecordingWorkerClient()
        app = master.create_app(
            task_executor=ImmediateExecutor(),
            worker_client=self.worker,
            start_monitor=False,
        )
        self.client = app.test_client()
        response = self.client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "node-01-host",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )
        self.assertEqual(response.status_code, 201)

    def test_one_available_node_executes_original_range_and_keeps_result_field(self) -> None:
        response = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 10})
        self.assertEqual(response.status_code, 201)
        task_id = response.get_json()["task_id"]

        task = self.client.get(f"/api/tasks/{task_id}").get_json()
        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["assigned_node"], "node-01")
        self.assertEqual(task["total_subtasks"], 1)
        self.assertEqual(task["result"], 55)
        self.assertEqual(task["final_result"], 55)
        self.assertEqual(self.worker.payloads[0]["parameters"], {"start": 1, "end": 10})

    def test_invalid_stage2_task_is_still_rejected(self) -> None:
        response = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 9, "end": 1})
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.get_json())


if __name__ == "__main__":
    unittest.main()
