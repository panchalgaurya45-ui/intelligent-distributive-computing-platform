"""IDCP master service for Stages 1-3.

Stage 1 node state and Stage 2/3 task state are deliberately stored in
separate in-memory registries. A later stage can replace either registry or
the simple round-robin scheduler without changing the HTTP API.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Protocol

from flask import Flask, jsonify, request


LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("idcp.master")

TASK_TYPE_SUM_RANGE = "sum_range"
NODE_TASK_STATES = {"IDLE", "BUSY"}
MAX_SUBTASKS = 3


def utc_now() -> str:
    """Return an ISO 8601 UTC timestamp suitable for JSON responses."""
    return datetime.now(timezone.utc).isoformat()


class NodeRegistry:
    """Thread-safe, in-memory registry for worker node availability and state."""

    def __init__(self, heartbeat_timeout_seconds: float) -> None:
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._nodes: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        node_id: str,
        hostname: str,
        platform: str,
        worker_url: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Register or refresh a node and return a copy plus whether it is new."""
        with self._lock:
            existing = self._nodes.get(node_id)
            now = time.monotonic()
            if existing is None:
                node = {
                    "node_id": node_id,
                    "hostname": hostname,
                    "platform": platform,
                    "worker_url": worker_url,
                    "status": "ONLINE",
                    "task_state": "IDLE",
                    "cpu_percent": None,
                    "memory_percent": None,
                    "available_memory": None,
                    "last_heartbeat": None,
                    "registered_at": utc_now(),
                    "_last_seen_monotonic": now,
                }
                self._nodes[node_id] = node
                logger.info("Node registered: %s (%s, %s)", node_id, hostname, platform)
                return self._public_copy(node), True

            was_offline = existing["status"] == "OFFLINE"
            existing.update(
                hostname=hostname,
                platform=platform,
                status="ONLINE",
                _last_seen_monotonic=now,
            )
            # Keep an already-known endpoint when an older Stage 1 agent refreshes.
            if worker_url is not None:
                existing["worker_url"] = worker_url
            if was_offline:
                logger.info("Node is ONLINE again after registration: %s", node_id)
            else:
                logger.info("Node registration refreshed: %s", node_id)
            return self._public_copy(existing), False

    def heartbeat(
        self,
        node_id: str,
        hostname: str,
        platform: str,
        cpu_percent: float,
        memory_percent: float,
        available_memory: int,
        timestamp: str,
    ) -> dict[str, Any] | None:
        """Store a heartbeat, or return None if its node has not registered."""
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return None

            was_offline = node["status"] == "OFFLINE"
            node.update(
                hostname=hostname,
                platform=platform,
                status="ONLINE",
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                available_memory=available_memory,
                last_heartbeat=timestamp,
                _last_seen_monotonic=time.monotonic(),
            )
            if was_offline:
                logger.info("Node is ONLINE again after heartbeat: %s", node_id)
            logger.info(
                "Heartbeat received from %s (CPU %.1f%%, memory %.1f%%)",
                node_id,
                cpu_percent,
                memory_percent,
            )
            return self._public_copy(node)

    def mark_timed_out_nodes_offline(self) -> list[str]:
        """Mark nodes that have missed the configured heartbeat window OFFLINE."""
        now = time.monotonic()
        offline_nodes: list[str] = []
        with self._lock:
            for node in self._nodes.values():
                timed_out = now - node["_last_seen_monotonic"] > self.heartbeat_timeout_seconds
                if node["status"] == "ONLINE" and timed_out:
                    node["status"] = "OFFLINE"
                    offline_nodes.append(node["node_id"])

        for node_id in offline_nodes:
            logger.warning("Node became OFFLINE after heartbeat timeout: %s", node_id)
        return offline_nodes

    def claim_node(self, node_id: str) -> dict[str, Any] | None:
        """Atomically mark a schedulable node BUSY and return its endpoint data."""
        self.mark_timed_out_nodes_offline()
        with self._lock:
            node = self._nodes.get(node_id)
            if (
                node is None
                or node["status"] != "ONLINE"
                or node["task_state"] != "IDLE"
                or not node.get("worker_url")
            ):
                return None
            node["task_state"] = "BUSY"
            return self._public_copy(node)

    def set_task_state(self, node_id: str, task_state: str) -> None:
        """Set a node's simple Stage 2 task availability state."""
        if task_state not in NODE_TASK_STATES:
            raise ValueError(f"Unsupported node task state: {task_state}")
        with self._lock:
            node = self._nodes.get(node_id)
            if node is not None:
                node["task_state"] = task_state

    def node_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._nodes)

    def list_nodes(self) -> list[dict[str, Any]]:
        self.mark_timed_out_nodes_offline()
        with self._lock:
            return [self._public_copy(self._nodes[node_id]) for node_id in sorted(self._nodes)]

    def counts(self) -> tuple[int, int]:
        self.mark_timed_out_nodes_offline()
        with self._lock:
            total = len(self._nodes)
            online = sum(node["status"] == "ONLINE" for node in self._nodes.values())
            return total, online

    @staticmethod
    def _public_copy(node: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in node.items() if not key.startswith("_")}


class RoundRobinScheduler:
    """Choose ONLINE, IDLE workers in round-robin order.

    Stage 2 calls ``select_node`` for a single workload. Stage 3 calls
    ``select_nodes`` to atomically claim a set of workers for one parent task.
    A future risk-aware scheduler can retain these interfaces.
    """

    def __init__(self, nodes: NodeRegistry) -> None:
        self.nodes = nodes
        self._last_selected_node_id: str | None = None
        self._lock = threading.Lock()

    def select_node(self) -> dict[str, Any] | None:
        """Claim one available worker, skipping OFFLINE and BUSY nodes."""
        selected = self.select_nodes(1)
        return selected[0] if selected else None

    def select_nodes(self, maximum_nodes: int) -> list[dict[str, Any]]:
        """Claim up to ``maximum_nodes`` distinct workers in round-robin order."""
        if maximum_nodes <= 0:
            return []
        with self._lock:
            node_ids = self.nodes.node_ids()
            if not node_ids:
                return []

            try:
                start_index = (node_ids.index(self._last_selected_node_id) + 1) % len(node_ids)
            except ValueError:
                start_index = 0

            selected: list[dict[str, Any]] = []
            for offset in range(len(node_ids)):
                node_id = node_ids[(start_index + offset) % len(node_ids)]
                node = self.nodes.claim_node(node_id)
                if node is not None:
                    self._last_selected_node_id = node_id
                    selected.append(node)
                    if len(selected) == maximum_nodes:
                        break
            if selected:
                logger.info("Round-robin selected nodes: %s", ", ".join(node["node_id"] for node in selected))
            return selected


class TaskRegistry:
    """Thread-safe, in-memory parent-task and subtask state for Stage 3."""

    def __init__(self) -> None:
        self._tasks: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def create(self, task_type: str, start: int, end: int) -> dict[str, Any]:
        task_id = f"T-{uuid.uuid4().hex[:12]}"
        task = {
            "task_id": task_id,
            "task_type": task_type,
            "start": start,
            "end": end,
            "assigned_node": None,
            "status": "CREATED",
            "result": None,
            "final_result": None,
            "error": None,
            "total_subtasks": 0,
            "completed_subtasks": 0,
            "failed_subtasks": 0,
            "subtasks": [],
            "created_at": utc_now(),
            "started_at": None,
            "completed_at": None,
        }
        with self._lock:
            self._tasks[task_id] = task
        logger.info("Task created: %s (%s, %s..%s)", task_id, task_type, start, end)
        return deepcopy(task)

    def create_subtasks(
        self,
        task_id: str,
        assignments: list[tuple[dict[str, Any], int, int]],
    ) -> list[dict[str, Any]] | None:
        """Create contiguous-range subtasks and record their selected workers."""
        if not assignments:
            return None
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task["status"] != "CREATED":
                return None

            created_subtasks: list[dict[str, Any]] = []
            for index, (node, start, end) in enumerate(assignments, start=1):
                subtask = {
                    "subtask_id": f"{task_id}-S{index}",
                    "parent_task_id": task_id,
                    "task_type": task["task_type"],
                    "start": start,
                    "end": end,
                    "assigned_node": node["node_id"],
                    "status": "ASSIGNED",
                    "result": None,
                    "error": None,
                    "created_at": utc_now(),
                    "started_at": None,
                    "completed_at": None,
                }
                created_subtasks.append(subtask)

            task.update(
                status="SPLIT",
                assigned_node=created_subtasks[0]["assigned_node"] if len(created_subtasks) == 1 else None,
                total_subtasks=len(created_subtasks),
                subtasks=created_subtasks,
            )
        logger.info("Task %s split into %s subtask(s)", task_id, len(created_subtasks))
        return deepcopy(created_subtasks)

    def mark_subtask_running(self, task_id: str, subtask_id: str) -> bool:
        with self._lock:
            task = self._tasks.get(task_id)
            subtask = self._find_subtask(task, subtask_id)
            if subtask is None or subtask["status"] != "ASSIGNED":
                return False
            subtask.update(status="RUNNING", started_at=utc_now())
            if task["status"] == "SPLIT":
                task.update(status="RUNNING", started_at=utc_now())
        logger.info("Subtask %s is RUNNING", subtask_id)
        return True

    def complete_subtask(self, task_id: str, subtask_id: str, result: int) -> bool:
        """Store a worker result and aggregate only after every subtask succeeds."""
        with self._lock:
            task = self._tasks.get(task_id)
            subtask = self._find_subtask(task, subtask_id)
            if subtask is None or subtask["status"] != "RUNNING":
                return False
            subtask.update(status="COMPLETED", result=result, completed_at=utc_now())
            task["completed_subtasks"] = sum(
                candidate["status"] == "COMPLETED" for candidate in task["subtasks"]
            )
            logger.info("Subtask %s COMPLETED", subtask_id)

            if task["status"] == "FAILED":
                return True
            if task["completed_subtasks"] != task["total_subtasks"]:
                return True

            # The master aggregates worker-returned partial results; it never
            # recalculates the original range itself.
            task["status"] = "AGGREGATING"
            final_result = sum(candidate["result"] for candidate in task["subtasks"])
            task.update(
                status="COMPLETED",
                final_result=final_result,
                # ``result`` remains for Stage 2 clients that read this field.
                result=final_result,
                completed_at=utc_now(),
            )
        logger.info("Task %s aggregated and COMPLETED", task_id)
        return True

    def fail_subtask(self, task_id: str, subtask_id: str, error: str) -> bool:
        with self._lock:
            task = self._tasks.get(task_id)
            subtask = self._find_subtask(task, subtask_id)
            if subtask is None or subtask["status"] in {"COMPLETED", "FAILED"}:
                return False
            subtask.update(status="FAILED", error=error, completed_at=utc_now())
            task.update(
                status="FAILED",
                error=f"Subtask {subtask_id} failed: {error}",
                failed_subtasks=sum(candidate["status"] == "FAILED" for candidate in task["subtasks"]),
                completed_at=utc_now(),
            )
        logger.warning("Subtask %s FAILED: %s", subtask_id, error)
        return True

    def fail_parent(self, task_id: str, error: str) -> bool:
        """Fail a parent before subtasks exist, for example with no workers."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task["status"] in {"COMPLETED", "FAILED"}:
                return False
            task.update(status="FAILED", error=error, completed_at=utc_now())
        logger.warning("Task %s FAILED: %s", task_id, error)
        return True

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            return deepcopy(task) if task is not None else None

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [deepcopy(task) for task in self._tasks.values()]

    @staticmethod
    def _find_subtask(task: dict[str, Any] | None, subtask_id: str) -> dict[str, Any] | None:
        if task is None:
            return None
        return next((subtask for subtask in task["subtasks"] if subtask["subtask_id"] == subtask_id), None)


class WorkerTaskError(Exception):
    """A safe, user-facing summary of a worker execution problem."""


class WorkerTaskClient:
    """Small HTTP boundary between master dispatch and a node agent."""

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds

    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        request_body = json.dumps(payload).encode("utf-8")
        request_object = urllib.request.Request(
            f"{worker_url.rstrip('/')}/api/tasks/execute",
            data=request_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request_object, timeout=self.timeout_seconds) as response:
                response_body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            logger.warning("Worker rejected task %s with HTTP %s", payload["task_id"], exc.code)
            raise WorkerTaskError("Worker rejected the task") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            logger.warning("Worker communication failed for task %s: %s", payload["task_id"], exc)
            raise WorkerTaskError("Worker could not be reached") from exc

        try:
            result_payload = json.loads(response_body)
            result = result_payload.get("result")
            if result_payload.get("status") != "COMPLETED" or isinstance(result, bool) or not isinstance(result, int):
                raise ValueError("invalid worker response")
            return result_payload["result"]
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.warning("Worker returned an invalid response for task %s", payload["task_id"])
            raise WorkerTaskError("Worker returned an invalid result") from exc


class TaskSubmitter(Protocol):
    """Subset of an executor used for dispatch; keeps tests lightweight."""

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any: ...


def parse_positive_number(value: Any, field_name: str, *, maximum: float | None = None) -> float:
    """Validate and convert JSON numeric fields, rejecting NaN and infinity."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number") from exc
    if not math.isfinite(number) or number < 0 or (maximum is not None and number > maximum):
        limit = f" between 0 and {maximum:g}" if maximum is not None else " greater than or equal to 0"
        raise ValueError(f"{field_name} must be{limit}")
    return number


def required_text(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required and must be a non-empty string")
    return value.strip()


def optional_worker_url(payload: dict[str, Any]) -> str | None:
    value = payload.get("worker_url")
    if value is None:
        return None
    if not isinstance(value, str) or not value.startswith(("http://", "https://")):
        raise ValueError("worker_url must be an HTTP(S) URL")
    return value.rstrip("/")


def parse_sum_range_task(payload: dict[str, Any]) -> tuple[str, int, int]:
    """Validate the one intentionally small Stage 2 task contract."""
    task_type = required_text(payload, "task_type")
    if task_type != TASK_TYPE_SUM_RANGE:
        raise ValueError(f"Unsupported task_type: {task_type}")

    start = payload.get("start")
    end = payload.get("end")
    if isinstance(start, bool) or not isinstance(start, int):
        raise ValueError("start must be an integer")
    if isinstance(end, bool) or not isinstance(end, int):
        raise ValueError("end must be an integer")
    if start > end:
        raise ValueError("start must be less than or equal to end")
    return task_type, start, end


def split_range(start: int, end: int, parts: int) -> list[tuple[int, int]]:
    """Split an inclusive integer range into balanced, contiguous non-empty parts.

    Extra values are assigned to the earlier ranges, so ``1..10`` split across
    three workers becomes ``1..4``, ``5..7``, and ``8..10``.
    """
    if parts <= 0:
        raise ValueError("parts must be greater than zero")
    total_values = end - start + 1
    if total_values <= 0:
        raise ValueError("range must contain at least one value")
    if parts > total_values:
        raise ValueError("parts cannot exceed the number of values in the range")

    base_size, remainder = divmod(total_values, parts)
    ranges: list[tuple[int, int]] = []
    current_start = start
    for index in range(parts):
        size = base_size + (1 if index < remainder else 0)
        current_end = current_start + size - 1
        ranges.append((current_start, current_end))
        current_start = current_end + 1
    return ranges


def require_json_object() -> tuple[dict[str, Any] | None, tuple[Any, int] | None]:
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return None, (jsonify(error="Request body must be a JSON object"), 400)
    return payload, None


def create_app(
    heartbeat_timeout_seconds: float | None = None,
    offline_check_interval_seconds: float | None = None,
    worker_task_timeout_seconds: float | None = None,
    task_executor: TaskSubmitter | None = None,
    worker_client: WorkerTaskClient | None = None,
    start_monitor: bool = True,
) -> Flask:
    """Create the master app; optional dependencies make API tests self-contained."""
    timeout = heartbeat_timeout_seconds if heartbeat_timeout_seconds is not None else float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))
    check_interval = offline_check_interval_seconds if offline_check_interval_seconds is not None else float(os.getenv("OFFLINE_CHECK_INTERVAL_SECONDS", "2"))
    worker_timeout = worker_task_timeout_seconds if worker_task_timeout_seconds is not None else float(os.getenv("WORKER_TASK_TIMEOUT_SECONDS", "30"))
    if timeout <= 0 or check_interval <= 0 or worker_timeout <= 0:
        raise ValueError("Timeout values must be positive")

    app = Flask(__name__)
    nodes = NodeRegistry(timeout)
    tasks = TaskRegistry()
    scheduler = RoundRobinScheduler(nodes)
    executor = task_executor or ThreadPoolExecutor(
        max_workers=int(os.getenv("TASK_DISPATCH_WORKERS", "4")),
        thread_name_prefix="task-dispatch",
    )
    client = worker_client or WorkerTaskClient(worker_timeout)
    app.config.update(
        NODE_REGISTRY=nodes,
        TASK_REGISTRY=tasks,
        TASK_SCHEDULER=scheduler,
        TASK_EXECUTOR=executor,
        HEARTBEAT_TIMEOUT_SECONDS=timeout,
    )

    def dispatch_subtask(parent_task_id: str, subtask: dict[str, Any], node: dict[str, Any]) -> None:
        """Execute one real range on one claimed worker without blocking Flask."""
        subtask_id = subtask["subtask_id"]
        node_id = node["node_id"]
        try:
            if not tasks.mark_subtask_running(parent_task_id, subtask_id):
                return
            result = client.execute(
                node["worker_url"],
                {
                    "task_id": subtask_id,
                    "task_type": subtask["task_type"],
                    "parameters": {"start": subtask["start"], "end": subtask["end"]},
                },
            )
            tasks.complete_subtask(parent_task_id, subtask_id, result)
        except WorkerTaskError as exc:
            tasks.fail_subtask(parent_task_id, subtask_id, str(exc))
        except Exception:
            logger.exception("Unexpected dispatch failure for subtask %s", subtask_id)
            tasks.fail_subtask(parent_task_id, subtask_id, "Subtask execution failed")
        finally:
            nodes.set_task_state(node_id, "IDLE")

    def dispatch_parent_task(task_id: str) -> None:
        """Split a parent task and schedule every subtask on a real worker."""
        task = tasks.get(task_id)
        if task is None:
            return

        desired_subtasks = min(MAX_SUBTASKS, task["end"] - task["start"] + 1)
        selected_nodes = scheduler.select_nodes(desired_subtasks)
        if not selected_nodes:
            tasks.fail_parent(task_id, "No ONLINE, IDLE worker node is available")
            return

        submitted_node_ids: set[str] = set()
        try:
            ranges = split_range(task["start"], task["end"], len(selected_nodes))
            assignments = [
                (node, range_start, range_end)
                for node, (range_start, range_end) in zip(selected_nodes, ranges, strict=True)
            ]
            subtasks = tasks.create_subtasks(task_id, assignments)
            if subtasks is None:
                raise RuntimeError("Task could not be split")
            for subtask, node in zip(subtasks, selected_nodes, strict=True):
                executor.submit(dispatch_subtask, task_id, subtask, node)
                submitted_node_ids.add(node["node_id"])
        except Exception:
            logger.exception("Unexpected split/dispatch failure for task %s", task_id)
            tasks.fail_parent(task_id, "Task could not be split or dispatched")
            for node in selected_nodes:
                # Do not incorrectly free a node whose subtask was already
                # submitted and may still be executing.
                if node["node_id"] not in submitted_node_ids:
                    nodes.set_task_state(node["node_id"], "IDLE")

    @app.post("/api/nodes/register")
    def register_node() -> tuple[Any, int]:
        payload, error = require_json_object()
        if error:
            return error
        try:
            node, is_new = nodes.register(
                required_text(payload, "node_id"),
                required_text(payload, "hostname"),
                required_text(payload, "platform"),
                optional_worker_url(payload),
            )
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        return jsonify(message="Node registered" if is_new else "Node registration refreshed", node=node), 201 if is_new else 200

    @app.post("/api/nodes/heartbeat")
    def receive_heartbeat() -> tuple[Any, int]:
        payload, error = require_json_object()
        if error:
            return error
        try:
            node_id = required_text(payload, "node_id")
            hostname = required_text(payload, "hostname")
            platform = required_text(payload, "platform")
            timestamp = required_text(payload, "timestamp")
            cpu_percent = parse_positive_number(payload.get("cpu_percent"), "cpu_percent", maximum=100)
            memory_percent = parse_positive_number(payload.get("memory_percent"), "memory_percent", maximum=100)
            available_memory_value = parse_positive_number(payload.get("available_memory"), "available_memory")
            if not available_memory_value.is_integer():
                raise ValueError("available_memory must be a whole number of bytes")
            available_memory = int(available_memory_value)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400

        node = nodes.heartbeat(
            node_id,
            hostname,
            platform,
            cpu_percent,
            memory_percent,
            available_memory,
            timestamp,
        )
        if node is None:
            return jsonify(error="Node is not registered", node_id=node_id), 404
        return jsonify(message="Heartbeat accepted", node=node), 200

    @app.get("/api/nodes")
    def list_nodes() -> tuple[Any, int]:
        node_list = nodes.list_nodes()
        return jsonify(nodes=node_list, count=len(node_list)), 200

    @app.get("/api/health")
    def health() -> tuple[Any, int]:
        total_nodes, online_nodes = nodes.counts()
        return (
            jsonify(
                status="healthy",
                service="idcp-master",
                total_nodes=total_nodes,
                online_nodes=online_nodes,
                timestamp=utc_now(),
            ),
            200,
        )

    @app.post("/api/tasks")
    def create_task() -> tuple[Any, int]:
        payload, error = require_json_object()
        if error:
            return error
        try:
            task_type, start, end = parse_sum_range_task(payload)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400

        task = tasks.create(task_type, start, end)
        # The initial response is always CREATED; lifecycle advances asynchronously.
        executor.submit(dispatch_parent_task, task["task_id"])
        return jsonify(task_id=task["task_id"], status="CREATED"), 201

    @app.get("/api/tasks")
    def list_tasks() -> tuple[Any, int]:
        task_list = tasks.list()
        return jsonify(tasks=task_list, count=len(task_list)), 200

    @app.get("/api/tasks/<task_id>")
    def get_task(task_id: str) -> tuple[Any, int]:
        task = tasks.get(task_id)
        if task is None:
            return jsonify(error="Task not found", task_id=task_id), 404
        return jsonify(task), 200

    if start_monitor:
        def offline_monitor() -> None:
            while True:
                time.sleep(check_interval)
                nodes.mark_timed_out_nodes_offline()

        monitor = threading.Thread(target=offline_monitor, name="offline-monitor", daemon=True)
        monitor.start()

    logger.info(
        "IDCP master initialized (heartbeat timeout: %ss, worker task timeout: %ss)",
        timeout,
        worker_timeout,
    )
    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    logger.info("Starting IDCP master on 0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
