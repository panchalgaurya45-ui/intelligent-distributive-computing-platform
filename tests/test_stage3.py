"""Lightweight Stage 3 tests using unittest and project dependencies only."""

from __future__ import annotations

import importlib.util
import sys
import time
import types
import unittest
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
import app as master  # noqa: E402


def load_agent_module() -> Any:
    # The local Codex host uses MSYS Python 3.14, for which the pinned psutil
    # release cannot compile. Task execution does not call psutil (only the
    # separate Stage 1 metric heartbeat path does), so a test-only empty module
    # lets this suite exercise the real worker task implementation. Docker uses
    # Python 3.12 and installs the actual pinned psutil package.
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
    """Runs background work synchronously to make state assertions stable."""

    def submit(self, function: Any, /, *args: Any, **kwargs: Any) -> None:
        function(*args, **kwargs)


class CalculatingWorkerClient:
    """Test worker boundary that performs the same real worker computation."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        self.calls.append((worker_url, payload))
        parameters = payload["parameters"]
        return agent.sum_range(parameters["start"], parameters["end"])


class FailingWorkerClient:
    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        raise master.WorkerTaskError("Worker rejected the task")


class Stage3RangeSplitTests(unittest.TestCase):
    def test_exactly_divisible_range(self) -> None:
        self.assertEqual(master.split_range(1, 12, 3), [(1, 4), (5, 8), (9, 12)])

    def test_non_even_range_has_no_gaps_or_overlaps(self) -> None:
        ranges = master.split_range(1, 10, 3)
        self.assertEqual(ranges, [(1, 4), (5, 7), (8, 10)])
        values = [value for start, end in ranges for value in range(start, end + 1)]
        self.assertEqual(values, list(range(1, 11)))
        self.assertEqual(len(values), len(set(values)))


class Stage3TaskApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker_client = CalculatingWorkerClient()
        self.app = master.create_app(
            task_executor=ImmediateExecutor(),
            worker_client=self.worker_client,
            start_monitor=False,
        )
        self.client = self.app.test_client()

    def register_nodes(self, count: int) -> None:
        for index in range(1, count + 1):
            node_id = f"node-{index:02d}"
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

    def submit(self, start: int, end: int) -> dict[str, Any]:
        response = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": start, "end": end})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.get_json()["status"], "CREATED")
        task_id = response.get_json()["task_id"]
        task_response = self.client.get(f"/api/tasks/{task_id}")
        self.assertEqual(task_response.status_code, 200)
        return task_response.get_json()

    def test_three_workers_receive_three_subtasks_and_master_aggregates(self) -> None:
        self.register_nodes(3)
        task = self.submit(1, 1_000_000)

        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["total_subtasks"], 3)
        self.assertEqual(task["completed_subtasks"], 3)
        self.assertEqual(task["failed_subtasks"], 0)
        self.assertEqual(task["final_result"], 500_000_500_000)
        self.assertEqual(task["result"], 500_000_500_000)  # Stage 2 compatibility.
        self.assertEqual(
            [(subtask["start"], subtask["end"]) for subtask in task["subtasks"]],
            [(1, 333_334), (333_335, 666_667), (666_668, 1_000_000)],
        )
        self.assertEqual(
            [subtask["assigned_node"] for subtask in task["subtasks"]],
            ["node-01", "node-02", "node-03"],
        )
        self.assertEqual(sum(subtask["result"] for subtask in task["subtasks"]), task["final_result"])
        self.assertEqual(len(self.worker_client.calls), 3)
        nodes = self.client.get("/api/nodes").get_json()["nodes"]
        self.assertTrue(all(node["task_state"] == "IDLE" for node in nodes))

    def test_subtask_count_matches_available_workers(self) -> None:
        self.register_nodes(2)
        task = self.submit(1, 10)
        self.assertEqual(task["total_subtasks"], 2)
        self.assertEqual(task["completed_subtasks"], 2)
        self.assertEqual([(subtask["start"], subtask["end"]) for subtask in task["subtasks"]], [(1, 5), (6, 10)])

    def test_stage2_single_node_execution_remains_compatible(self) -> None:
        self.register_nodes(1)
        task = self.submit(1, 10)
        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["total_subtasks"], 1)
        self.assertEqual(task["assigned_node"], "node-01")
        self.assertEqual(task["result"], 55)
        self.assertEqual(task["final_result"], 55)

    def test_parent_fails_when_a_subtask_fails(self) -> None:
        app = master.create_app(
            task_executor=ImmediateExecutor(),
            worker_client=FailingWorkerClient(),
            start_monitor=False,
        )
        client = app.test_client()
        client.post(
            "/api/nodes/register",
            json={
                "node_id": "node-01",
                "hostname": "node-01-host",
                "platform": "test-platform",
                "worker_url": "http://node-01:5001",
            },
        )
        response = client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 10})
        task = client.get(f"/api/tasks/{response.get_json()['task_id']}").get_json()
        self.assertEqual(task["status"], "FAILED")
        self.assertEqual(task["failed_subtasks"], 1)
        self.assertIn("failed", task["error"].lower())
        self.assertEqual(client.get("/api/nodes").get_json()["nodes"][0]["task_state"], "IDLE")

    def test_parent_fails_cleanly_without_an_available_worker(self) -> None:
        response = self.client.post("/api/tasks", json={"task_type": "sum_range", "start": 1, "end": 10})
        task = self.client.get(f"/api/tasks/{response.get_json()['task_id']}").get_json()
        self.assertEqual(task["status"], "FAILED")
        self.assertEqual(task["total_subtasks"], 0)
        self.assertIn("No ONLINE, IDLE worker", task["error"])

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

    def test_worker_executes_the_subtask_range(self) -> None:
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
                "task_id": "T-test-S1",
                "task_type": "sum_range",
                "parameters": {"start": 5, "end": 7},
            }
        )
        self.assertEqual(response, {"task_id": "T-test-S1", "status": "COMPLETED", "result": 18})

    def test_offline_node_is_not_selected(self) -> None:
        registry = master.NodeRegistry(heartbeat_timeout_seconds=0.05)
        registry.register("node-01", "host-01", "test", "http://node-01:5001")
        time.sleep(0.06)
        registry.register("node-02", "host-02", "test", "http://node-02:5001")
        selected = master.RoundRobinScheduler(registry).select_nodes(3)
        self.assertEqual([node["node_id"] for node in selected], ["node-02"])

    def test_claimed_nodes_are_marked_busy(self) -> None:
        registry = master.NodeRegistry(heartbeat_timeout_seconds=5)
        registry.register("node-01", "host-01", "test", "http://node-01:5001")
        registry.register("node-02", "host-02", "test", "http://node-02:5001")
        selected = master.RoundRobinScheduler(registry).select_nodes(2)
        selected_ids = {node["node_id"] for node in selected}
        states = {node["node_id"]: node["task_state"] for node in registry.list_nodes()}
        self.assertEqual(selected_ids, {"node-01", "node-02"})
        self.assertEqual(states, {"node-01": "BUSY", "node-02": "BUSY"})


if __name__ == "__main__":
    unittest.main()
