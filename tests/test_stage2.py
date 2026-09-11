"""Lightweight Stage 2 tests using only unittest and the project dependencies."""

from __future__ import annotations

import importlib.util
import sys
import time
import unittest
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
import app as master  # noqa: E402


def load_agent_module() -> Any:
    spec = importlib.util.spec_from_file_location("idcp_node_agent", PROJECT_ROOT / "node-agent" / "agent.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load node-agent module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


agent = load_agent_module()


class ImmediateExecutor:
    """Runs submitted work synchronously so API lifecycle assertions are stable."""

    def submit(self, function: Any, /, *args: Any, **kwargs: Any) -> None:
        function(*args, **kwargs)


class FakeWorkerClient:
    """A real calculator behind the master's worker-client boundary."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        self.calls.append((worker_url, payload))
        parameters = payload["parameters"]
        return agent.sum_range(parameters["start"], parameters["end"])


class Stage2TaskApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker_client = FakeWorkerClient()
        self.app = master.create_app(
            task_executor=ImmediateExecutor(),
            worker_client=self.worker_client,
            start_monitor=False,
        )
        self.client = self.app.test_client()

    def register_node(self, node_id: str) -> None:
        response = self.client.post(
            "/api/nodes/register",
            json={
                "node_id": node_id,
                "hostname": f"{node_id}-host",
                "platform": "test-platform",
                "worker_url": f"http://{node_id}:5001",
            },
        )
        self.assertEqual(response.status_code, 201)

    def test_sum_range_calculation(self) -> None:
        self.assertEqual(agent.sum_range(1, 1_000_000), 500_000_500_000)

    def test_worker_executes_validated_task(self) -> None:
        worker = agent.NodeAgent(
            node_id="node-test",
            master_url="http://master:5000",
            worker_url="http://node-test:5001",
            worker_bind_host="127.0.0.1",
            worker_port=5001,
            heartbeat_interval_seconds=5,
            request_timeout_seconds=3,
        )
        response = worker.execute_task(
            {
                "task_id": "T-test",
                "task_type": "sum_range",
                "parameters": {"start": 1, "end": 10},
            }
        )
        self.assertEqual(response, {"task_id": "T-test", "status": "COMPLETED", "result": 55})

    def test_invalid_task_is_rejected(self) -> None:
        cases = [
            {},
            {"task_type": "unknown", "start": 1, "end": 2},
            {"task_type": "sum_range", "start": 1.5, "end": 2},
            {"task_type": "sum_range", "start": 4, "end": 1},
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                response = self.client.post("/api/tasks", json=payload)
                self.assertEqual(response.status_code, 400)
                self.assertIn("error", response.get_json())

    def test_task_creation_execution_and_result_retrieval(self) -> None:
        self.register_node("node-01")
        response = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 10})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.get_json()["status"], "CREATED")

        task_id = response.get_json()["task_id"]
        task_response = self.client.get(f"/api/tasks/{task_id}")
        task = task_response.get_json()
        self.assertEqual(task_response.status_code, 200)
        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["assigned_node"], "node-01")
        self.assertEqual(task["result"], 55)
        self.assertIsNone(task["error"])
        self.assertEqual(len(self.worker_client.calls), 1)

    def test_round_robin_assigns_successive_tasks_to_successive_nodes(self) -> None:
        self.register_node("node-01")
        self.register_node("node-02")
        first = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 1}).get_json()
        second = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 2, "end": 2}).get_json()

        first_task = self.client.get(f"/api/tasks/{first['task_id']}").get_json()
        second_task = self.client.get(f"/api/tasks/{second['task_id']}").get_json()
        self.assertEqual(first_task["assigned_node"], "node-01")
        self.assertEqual(second_task["assigned_node"], "node-02")

    def test_offline_node_is_not_selected(self) -> None:
        registry = master.NodeRegistry(heartbeat_timeout_seconds=0.05)
        registry.register("node-01", "host-01", "test", "http://node-01:5001")
        time.sleep(0.06)
        registry.register("node-02", "host-02", "test", "http://node-02:5001")
        selected = master.RoundRobinScheduler(registry).select_node()
        self.assertIsNotNone(selected)
        self.assertEqual(selected["node_id"], "node-02")


if __name__ == "__main__":
    unittest.main()
