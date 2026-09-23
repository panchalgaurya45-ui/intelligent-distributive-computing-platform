"""IDCP master service for Stages 1-4C.

Stage 1 node state and Stage 2/3 task state are deliberately stored in
separate in-memory registries. A later stage can replace either registry or
the simple round-robin scheduler without changing the HTTP API.

Stage 4B added PostgreSQL persistence for node identity (Node rows) and
heartbeat history (NodeMetric rows).
Stage 4C adds PostgreSQL persistence for task execution history (Task rows),
subtask execution history (Subtask rows), and system audit events (Event rows).
The in-memory registries remain the authoritative source for scheduling;
the database is the durable audit log and historical record.
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
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Protocol

from flask import Flask, jsonify, request

from database import configure_database, db, initialize_database
from models import Event, Node, NodeMetric, Subtask, Task  # noqa: F401  # Register models before db.create_all().

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("idcp.master")

TASK_TYPE_SUM_RANGE = "sum_range"
NODE_TASK_STATES = {"IDLE", "BUSY"}
MAX_SUBTASKS = 3


def utc_now() -> str:
    """Return an ISO 8601 UTC timestamp suitable for JSON responses."""
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_timestamp(ts_str: str | None) -> datetime | None:
    """Safely parse an ISO 8601 timestamp string into a datetime object."""
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.now(timezone.utc)


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
    ) -> tuple[dict[str, Any], bool, bool]:
        """Register or refresh a node and return (public_copy, is_new, was_offline)."""
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
                return self._public_copy(node), True, False

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
            return self._public_copy(existing), False, was_offline

    def heartbeat(
        self,
        node_id: str,
        hostname: str,
        platform: str,
        cpu_percent: float,
        memory_percent: float,
        available_memory: int,
        timestamp: str,
    ) -> tuple[dict[str, Any] | None, bool]:
        """Store a heartbeat, or return (None, False) if its node has not registered."""
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return None, False

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
            return self._public_copy(node), was_offline

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
    """Choose ONLINE, IDLE workers in round-robin order."""

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

    def mark_subtask_running(self, task_id: str, subtask_id: str) -> tuple[bool, bool]:
        """Mark subtask running and return (success, parent_was_split_and_now_running)."""
        with self._lock:
            task = self._tasks.get(task_id)
            subtask = self._find_subtask(task, subtask_id)
            if subtask is None or subtask["status"] != "ASSIGNED":
                return False, False
            now_ts = utc_now()
            subtask.update(status="RUNNING", started_at=now_ts)
            parent_started = False
            if task["status"] == "SPLIT":
                task.update(status="RUNNING", started_at=now_ts)
                parent_started = True
        logger.info("Subtask %s is RUNNING", subtask_id)
        return True, parent_started

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

            task["status"] = "AGGREGATING"
            final_result = sum(candidate["result"] for candidate in task["subtasks"])
            task.update(
                status="COMPLETED",
                final_result=final_result,
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


class TaskSubmitter(Protocol):
    def submit(self, function: Any, /, *args: Any, **kwargs: Any) -> Any: ...


class WorkerTaskError(Exception):
    """Raised when a worker node cannot complete a task cleanly."""


class WorkerTaskClient:
    """HTTP client for dispatching subtasks to worker node agents."""

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds

    def execute(self, worker_url: str, payload: dict[str, Any]) -> int:
        endpoint = f"{worker_url.rstrip('/')}/api/tasks/execute"
        body = json.dumps(payload).encode("utf-8")
        request_obj = urllib.request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request_obj, timeout=self.timeout_seconds) as response:
                status_code = response.status
                raw_response = response.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise WorkerTaskError(f"Worker request failed: {exc}") from exc

        if status_code != 200:
            raise WorkerTaskError(f"Worker returned status code {status_code}: {raw_response}")

        try:
            data = json.loads(raw_response)
        except json.JSONDecodeError as exc:
            raise WorkerTaskError("Worker response was not valid JSON") from exc

        if not isinstance(data, dict):
            raise WorkerTaskError("Worker response body must be a JSON object")

        if data.get("status") != "COMPLETED" or "result" not in data:
            error_message = data.get("error", "Unknown worker execution error")
            raise WorkerTaskError(f"Worker task failed: {error_message}")

        try:
            return int(data["result"])
        except (TypeError, ValueError) as exc:
            raise WorkerTaskError("Worker result must be an integer") from exc


def required_text(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Field '{field_name}' must be a non-empty string")
    return value.strip()


def parse_positive_number(
    value: Any, field_name: str, maximum: float | None = None
) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"Field '{field_name}' must be a positive number")
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Field '{field_name}' must be a positive number")

    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError(f"Field '{field_name}' must be a positive number")
    if maximum is not None and numeric > maximum:
        raise ValueError(f"Field '{field_name}' cannot exceed {maximum}")
    return numeric


def optional_worker_url(payload: dict[str, Any]) -> str | None:
    value = payload.get("worker_url")
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Field 'worker_url' must be a non-empty string when provided")
    url = value.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("Field 'worker_url' must start with http:// or https://")
    return url


def parse_sum_range_task(payload: dict[str, Any]) -> tuple[str, int, int]:
    task_type = required_text(payload, "task_type")
    if task_type != TASK_TYPE_SUM_RANGE:
        raise ValueError(f"Unsupported task_type: {task_type}")

    params = payload.get("parameters")
    if isinstance(params, dict):
        start_val = params.get("start")
        end_val = params.get("end")
    else:
        start_val = payload.get("start")
        end_val = payload.get("end")

    if start_val is None or isinstance(start_val, bool) or not isinstance(start_val, int):
        raise ValueError("Parameter 'start' must be an integer")
    if end_val is None or isinstance(end_val, bool) or not isinstance(end_val, int):
        raise ValueError("Parameter 'end' must be an integer")

    if start_val > end_val:
        raise ValueError("Parameter 'start' must be less than or equal to 'end'")

    return task_type, start_val, end_val


def split_range(start: int, end: int, parts: int) -> list[tuple[int, int]]:
    """Split an inclusive range into ``parts`` contiguous sub-ranges."""
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


def _parse_heartbeat_timestamp(timestamp: str) -> datetime:
    """Convert the ISO 8601 timestamp string from a worker heartbeat payload."""
    try:
        return datetime.fromisoformat(timestamp)
    except ValueError:
        return datetime.now(timezone.utc)


def _persist_node_upsert(
    app: Flask, node: dict[str, Any], is_new: bool = False, was_offline: bool = False
) -> None:
    """Insert or update the Node row and log corresponding registration events."""
    with app.app_context():
        try:
            db_node = db.session.execute(
                db.select(Node).where(Node.node_id == node["node_id"])
            ).scalar_one_or_none()
            if db_node is None:
                db_node = Node(
                    node_id=node["node_id"],
                    hostname=node["hostname"],
                    platform=node["platform"],
                    status=node["status"],
                    worker_url=node.get("worker_url"),
                )
                db.session.add(db_node)
            else:
                db_node.hostname = node["hostname"]
                db_node.platform = node["platform"]
                db_node.status = node["status"]
                if node.get("worker_url") is not None:
                    db_node.worker_url = node["worker_url"]

            if is_new:
                db.session.add(
                    Event(
                        event_type="NODE_REGISTERED",
                        message=f"Node {node['node_id']} registered ({node['hostname']}, {node['platform']})",
                        node_id=node["node_id"],
                        severity="INFO",
                    )
                )
            elif was_offline:
                db.session.add(
                    Event(
                        event_type="NODE_ONLINE",
                        message=f"Node {node['node_id']} is ONLINE again after registration",
                        node_id=node["node_id"],
                        severity="INFO",
                    )
                )

            db.session.commit()
        except Exception:
            logger.exception(
                "DB write failed during registration of %s; in-memory state is intact",
                node["node_id"],
            )
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_heartbeat_metric(
    app: Flask,
    node_id: str,
    hostname: str,
    platform: str,
    cpu_percent: float,
    memory_percent: float,
    available_memory: int,
    hb_ts: datetime,
    was_offline: bool = False,
) -> None:
    """Insert one NodeMetric row and refresh the Node row after a heartbeat."""
    with app.app_context():
        try:
            metric = NodeMetric(
                node_id=node_id,
                timestamp=hb_ts,
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                available_memory=available_memory,
                heartbeat_latency=None,
            )
            db.session.add(metric)

            db_node = db.session.execute(
                db.select(Node).where(Node.node_id == node_id)
            ).scalar_one_or_none()
            if db_node is not None:
                db_node.hostname = hostname
                db_node.platform = platform
                db_node.status = "ONLINE"
                db_node.last_heartbeat = hb_ts

            if was_offline:
                db.session.add(
                    Event(
                        event_type="NODE_ONLINE",
                        message=f"Node {node_id} is ONLINE again after heartbeat",
                        node_id=node_id,
                        severity="INFO",
                        timestamp=hb_ts,
                    )
                )

            db.session.commit()
        except Exception:
            logger.exception(
                "DB write failed during heartbeat from %s; in-memory state is intact", node_id
            )
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_node_offline(app: Flask, offline_node_ids: list[str]) -> None:
    """Record NODE_OFFLINE event and update Node status in DB when timeout occurs."""
    if not offline_node_ids:
        return
    with app.app_context():
        try:
            for node_id in offline_node_ids:
                db_node = db.session.execute(
                    db.select(Node).where(Node.node_id == node_id)
                ).scalar_one_or_none()
                if db_node is not None:
                    db_node.status = "OFFLINE"
                db.session.add(
                    Event(
                        event_type="NODE_OFFLINE",
                        message=f"Node {node_id} marked OFFLINE due to heartbeat timeout",
                        node_id=node_id,
                        severity="WARNING",
                    )
                )
            db.session.commit()
        except Exception:
            logger.exception("DB write failed setting nodes offline: %s", offline_node_ids)
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_task_created(app: Flask, task: dict[str, Any]) -> None:
    """Persist a newly created Task row and TASK_CREATED event."""
    with app.app_context():
        try:
            ts = _parse_iso_timestamp(task.get("created_at")) or datetime.now(timezone.utc)
            db_task = Task(
                task_id=task["task_id"],
                task_type=task["task_type"],
                status=task["status"],
                start=task.get("start"),
                end=task.get("end"),
                total_subtasks=0,
                completed_subtasks=0,
                failed_subtasks=0,
                created_at=ts,
            )
            db.session.add(db_task)
            db.session.add(
                Event(
                    event_type="TASK_CREATED",
                    message=f"Task {task['task_id']} created ({task['task_type']}, {task.get('start')}..{task.get('end')})",
                    task_id=task["task_id"],
                    severity="INFO",
                    timestamp=ts,
                )
            )
            db.session.commit()
        except Exception:
            logger.exception("DB write failed creating task %s", task.get("task_id"))
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_task_split_and_subtasks(
    app: Flask,
    task_id: str,
    total_subtasks: int,
    created_subtasks: list[dict[str, Any]],
) -> None:
    """Persist task status SPLIT, Subtask rows, and split/assignment events."""
    with app.app_context():
        try:
            db_task = db.session.execute(
                db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            if db_task is not None:
                db_task.status = "SPLIT"
                db_task.total_subtasks = total_subtasks

            db.session.add(
                Event(
                    event_type="TASK_ASSIGNED",
                    message=f"Task {task_id} split into {total_subtasks} subtask(s)",
                    task_id=task_id,
                    severity="INFO",
                )
            )

            for subtask in created_subtasks:
                ts = _parse_iso_timestamp(subtask.get("created_at")) or datetime.now(timezone.utc)
                db_subtask = Subtask(
                    subtask_id=subtask["subtask_id"],
                    parent_task_id=task_id,
                    node_id=subtask.get("assigned_node"),
                    task_type=subtask["task_type"],
                    status=subtask["status"],
                    start=subtask.get("start"),
                    end=subtask.get("end"),
                    created_at=ts,
                )
                db.session.add(db_subtask)

                db.session.add(
                    Event(
                        event_type="SUBTASK_CREATED",
                        message=f"Subtask {subtask['subtask_id']} created",
                        task_id=task_id,
                        subtask_id=subtask["subtask_id"],
                        node_id=subtask.get("assigned_node"),
                        severity="INFO",
                        timestamp=ts,
                    )
                )

                db.session.add(
                    Event(
                        event_type="SUBTASK_ASSIGNED",
                        message=f"Subtask {subtask['subtask_id']} assigned to node {subtask.get('assigned_node')}",
                        task_id=task_id,
                        subtask_id=subtask["subtask_id"],
                        node_id=subtask.get("assigned_node"),
                        severity="INFO",
                        timestamp=ts,
                    )
                )

            db.session.commit()
        except Exception:
            logger.exception("DB write failed persisting subtasks for task %s", task_id)
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_subtask_running(
    app: Flask,
    task_id: str,
    subtask_id: str,
    parent_started: bool,
) -> None:
    """Persist SUBTASK_STARTED event and optionally TASK_STARTED event."""
    with app.app_context():
        try:
            ts = datetime.now(timezone.utc)
            db_subtask = db.session.execute(
                db.select(Subtask).where(Subtask.subtask_id == subtask_id)
            ).scalar_one_or_none()
            node_id = None
            if db_subtask is not None:
                db_subtask.status = "RUNNING"
                db_subtask.started_at = ts
                node_id = db_subtask.node_id

            db.session.add(
                Event(
                    event_type="SUBTASK_STARTED",
                    message=f"Subtask {subtask_id} started execution",
                    task_id=task_id,
                    subtask_id=subtask_id,
                    node_id=node_id,
                    severity="INFO",
                    timestamp=ts,
                )
            )

            if parent_started:
                db_task = db.session.execute(
                    db.select(Task).where(Task.task_id == task_id)
                ).scalar_one_or_none()
                if db_task is not None:
                    db_task.status = "RUNNING"
                    db_task.started_at = ts
                db.session.add(
                    Event(
                        event_type="TASK_STARTED",
                        message=f"Task {task_id} execution started",
                        task_id=task_id,
                        severity="INFO",
                        timestamp=ts,
                    )
                )

            db.session.commit()
        except Exception:
            logger.exception("DB write failed marking subtask %s running", subtask_id)
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_subtask_completed(
    app: Flask,
    task_id: str,
    subtask_id: str,
    result: int,
    task_snapshot: dict[str, Any],
) -> None:
    """Persist SUBTASK_COMPLETED event, update subtask result, and update parent task if complete."""
    with app.app_context():
        try:
            ts = datetime.now(timezone.utc)
            db_subtask = db.session.execute(
                db.select(Subtask).where(Subtask.subtask_id == subtask_id)
            ).scalar_one_or_none()
            node_id = None
            if db_subtask is not None:
                db_subtask.status = "COMPLETED"
                db_subtask.result = result
                db_subtask.completed_at = ts
                node_id = db_subtask.node_id

            db.session.add(
                Event(
                    event_type="SUBTASK_COMPLETED",
                    message=f"Subtask {subtask_id} completed with result {result}",
                    task_id=task_id,
                    subtask_id=subtask_id,
                    node_id=node_id,
                    severity="INFO",
                    timestamp=ts,
                )
            )

            db_task = db.session.execute(
                db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            if db_task is not None:
                db_task.completed_subtasks = task_snapshot.get("completed_subtasks", 0)
                if task_snapshot.get("status") == "COMPLETED":
                    db_task.status = "COMPLETED"
                    db_task.final_result = task_snapshot.get("final_result")
                    comp_ts = _parse_iso_timestamp(task_snapshot.get("completed_at")) or ts
                    db_task.completed_at = comp_ts
                    db.session.add(
                        Event(
                            event_type="TASK_COMPLETED",
                            message=f"Task {task_id} completed with final result {task_snapshot.get('final_result')}",
                            task_id=task_id,
                            severity="INFO",
                            timestamp=comp_ts,
                        )
                    )

            db.session.commit()
        except Exception:
            logger.exception("DB write failed completing subtask %s", subtask_id)
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_subtask_failed(
    app: Flask,
    task_id: str,
    subtask_id: str,
    error: str,
    task_snapshot: dict[str, Any],
) -> None:
    """Persist SUBTASK_FAILED event, subtask error, and update parent task to FAILED."""
    with app.app_context():
        try:
            ts = datetime.now(timezone.utc)
            db_subtask = db.session.execute(
                db.select(Subtask).where(Subtask.subtask_id == subtask_id)
            ).scalar_one_or_none()
            node_id = None
            if db_subtask is not None:
                db_subtask.status = "FAILED"
                db_subtask.error = error
                db_subtask.completed_at = ts
                node_id = db_subtask.node_id

            db.session.add(
                Event(
                    event_type="SUBTASK_FAILED",
                    message=f"Subtask {subtask_id} failed: {error}",
                    task_id=task_id,
                    subtask_id=subtask_id,
                    node_id=node_id,
                    severity="ERROR",
                    timestamp=ts,
                )
            )

            db_task = db.session.execute(
                db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            if db_task is not None:
                db_task.status = "FAILED"
                db_task.failed_subtasks = task_snapshot.get("failed_subtasks", 1)
                db_task.error = task_snapshot.get("error")
                comp_ts = _parse_iso_timestamp(task_snapshot.get("completed_at")) or ts
                db_task.completed_at = comp_ts
                db.session.add(
                    Event(
                        event_type="TASK_FAILED",
                        message=f"Task {task_id} failed: {task_snapshot.get('error')}",
                        task_id=task_id,
                        severity="ERROR",
                        timestamp=comp_ts,
                    )
                )

            db.session.commit()
        except Exception:
            logger.exception("DB write failed marking subtask %s failed", subtask_id)
            try:
                db.session.rollback()
            except Exception:
                pass


def _persist_parent_failed(
    app: Flask,
    task_id: str,
    error: str,
) -> None:
    """Persist parent task failure when dispatch cannot split or find workers."""
    with app.app_context():
        try:
            ts = datetime.now(timezone.utc)
            db_task = db.session.execute(
                db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            if db_task is not None:
                db_task.status = "FAILED"
                db_task.error = error
                db_task.completed_at = ts
            db.session.add(
                Event(
                    event_type="TASK_FAILED",
                    message=f"Task {task_id} failed: {error}",
                    task_id=task_id,
                    severity="ERROR",
                    timestamp=ts,
                )
            )
            db.session.commit()
        except Exception:
            logger.exception("DB write failed failing parent task %s", task_id)
            try:
                db.session.rollback()
            except Exception:
                pass


def create_app(
    heartbeat_timeout_seconds: float | None = None,
    offline_check_interval_seconds: float | None = None,
    worker_task_timeout_seconds: float | None = None,
    task_executor: TaskSubmitter | None = None,
    worker_client: WorkerTaskClient | None = None,
    start_monitor: bool = True,
    database_uri: str | None = None,
    initialize_db: bool = True,
) -> Flask:
    """Create the master app; optional dependencies make API tests self-contained."""
    timeout = heartbeat_timeout_seconds if heartbeat_timeout_seconds is not None else float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))
    check_interval = offline_check_interval_seconds if offline_check_interval_seconds is not None else float(os.getenv("OFFLINE_CHECK_INTERVAL_SECONDS", "2"))
    worker_timeout = worker_task_timeout_seconds if worker_task_timeout_seconds is not None else float(os.getenv("WORKER_TASK_TIMEOUT_SECONDS", "30"))
    if timeout <= 0 or check_interval <= 0 or worker_timeout <= 0:
        raise ValueError("Timeout values must be positive")

    app = Flask(__name__)
    configure_database(app, database_uri)
    if initialize_db:
        initialize_database(app)

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
            marked, parent_started = tasks.mark_subtask_running(parent_task_id, subtask_id)
            if not marked:
                return
            _persist_subtask_running(app, parent_task_id, subtask_id, parent_started)

            result = client.execute(
                node["worker_url"],
                {
                    "task_id": subtask_id,
                    "task_type": subtask["task_type"],
                    "parameters": {"start": subtask["start"], "end": subtask["end"]},
                },
            )
            tasks.complete_subtask(parent_task_id, subtask_id, result)
            task_snapshot = tasks.get(parent_task_id) or {}
            _persist_subtask_completed(app, parent_task_id, subtask_id, result, task_snapshot)
        except WorkerTaskError as exc:
            tasks.fail_subtask(parent_task_id, subtask_id, str(exc))
            task_snapshot = tasks.get(parent_task_id) or {}
            _persist_subtask_failed(app, parent_task_id, subtask_id, str(exc), task_snapshot)
        except Exception:
            logger.exception("Unexpected dispatch failure for subtask %s", subtask_id)
            tasks.fail_subtask(parent_task_id, subtask_id, "Subtask execution failed")
            task_snapshot = tasks.get(parent_task_id) or {}
            _persist_subtask_failed(app, parent_task_id, subtask_id, "Subtask execution failed", task_snapshot)
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
            _persist_parent_failed(app, task_id, "No ONLINE, IDLE worker node is available")
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
            _persist_task_split_and_subtasks(app, task_id, len(subtasks), subtasks)
            for subtask, node in zip(subtasks, selected_nodes, strict=True):
                executor.submit(dispatch_subtask, task_id, subtask, node)
                submitted_node_ids.add(node["node_id"])
        except Exception:
            logger.exception("Unexpected split/dispatch failure for task %s", task_id)
            tasks.fail_parent(task_id, "Task could not be split or dispatched")
            _persist_parent_failed(app, task_id, "Task could not be split or dispatched")
            for node in selected_nodes:
                if node["node_id"] not in submitted_node_ids:
                    nodes.set_task_state(node["node_id"], "IDLE")

    # -----------------------------------------------------------------------
    # Node endpoints
    # -----------------------------------------------------------------------

    @app.post("/api/nodes/register")
    def register_node() -> tuple[Any, int]:
        payload, error = require_json_object()
        if error:
            return error
        try:
            node, is_new, was_offline = nodes.register(
                required_text(payload, "node_id"),
                required_text(payload, "hostname"),
                required_text(payload, "platform"),
                optional_worker_url(payload),
            )
        except ValueError as exc:
            return jsonify(error=str(exc)), 400

        _persist_node_upsert(app, node, is_new=is_new, was_offline=was_offline)

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

        node, was_offline = nodes.heartbeat(
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

        hb_ts = _parse_heartbeat_timestamp(timestamp)
        _persist_heartbeat_metric(
            app, node_id, hostname, platform, cpu_percent, memory_percent, available_memory, hb_ts, was_offline=was_offline
        )

        return jsonify(message="Heartbeat accepted", node=node), 200

    @app.get("/api/nodes")
    def list_nodes() -> tuple[Any, int]:
        offline_nodes = nodes.mark_timed_out_nodes_offline()
        _persist_node_offline(app, offline_nodes)
        node_list = nodes.list_nodes()
        return jsonify(nodes=node_list, count=len(node_list)), 200

    @app.get("/api/nodes/<node_id>")
    def get_node(node_id: str) -> tuple[Any, int]:
        """Return node state plus the latest persisted metric from PostgreSQL."""
        offline_nodes = nodes.mark_timed_out_nodes_offline()
        _persist_node_offline(app, offline_nodes)
        node_list = nodes.list_nodes()
        node = next((n for n in node_list if n["node_id"] == node_id), None)
        if node is None:
            return jsonify(error="Node not found", node_id=node_id), 404

        latest_metric: dict[str, Any] | None = None
        try:
            row = db.session.execute(
                db.select(NodeMetric)
                .where(NodeMetric.node_id == node_id)
                .order_by(NodeMetric.timestamp.desc())
                .limit(1)
            ).scalar_one_or_none()
            if row is not None:
                latest_metric = {
                    "timestamp": row.timestamp.isoformat(),
                    "cpu_percent": row.cpu_percent,
                    "memory_percent": row.memory_percent,
                    "available_memory": row.available_memory,
                    "heartbeat_latency": row.heartbeat_latency,
                }
        except Exception:
            logger.exception("DB read failed for latest metric of node %s", node_id)

        return jsonify(**node, latest_metric=latest_metric), 200

    @app.get("/api/nodes/<node_id>/metrics")
    def get_node_metrics(node_id: str) -> tuple[Any, int]:
        """Return paginated heartbeat history for a node, newest-first."""
        raw_limit = request.args.get("limit", "100")
        try:
            limit = int(raw_limit)
            if limit <= 0:
                raise ValueError("limit must be positive")
        except ValueError:
            return jsonify(error="limit must be a positive integer"), 400

        node_list = nodes.list_nodes()
        node_known = any(n["node_id"] == node_id for n in node_list)
        if not node_known:
            try:
                db_node = db.session.execute(
                    db.select(Node).where(Node.node_id == node_id)
                ).scalar_one_or_none()
                if db_node is None:
                    return jsonify(error="Node not found", node_id=node_id), 404
            except Exception:
                logger.exception("DB read failed checking existence of node %s", node_id)
                return jsonify(error="Node not found", node_id=node_id), 404

        metrics: list[dict[str, Any]] = []
        try:
            rows = db.session.execute(
                db.select(NodeMetric)
                .where(NodeMetric.node_id == node_id)
                .order_by(NodeMetric.timestamp.desc())
                .limit(limit)
            ).scalars().all()
            metrics = [
                {
                    "timestamp": row.timestamp.isoformat(),
                    "cpu_percent": row.cpu_percent,
                    "memory_percent": row.memory_percent,
                    "available_memory": row.available_memory,
                    "heartbeat_latency": row.heartbeat_latency,
                }
                for row in rows
            ]
        except Exception:
            logger.exception("DB read failed for metrics of node %s", node_id)

        return jsonify(node_id=node_id, count=len(metrics), metrics=metrics), 200

    # -----------------------------------------------------------------------
    # Health endpoint
    # -----------------------------------------------------------------------

    @app.get("/api/health")
    def health() -> tuple[Any, int]:
        offline_nodes = nodes.mark_timed_out_nodes_offline()
        _persist_node_offline(app, offline_nodes)
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

    # -----------------------------------------------------------------------
    # Task endpoints
    # -----------------------------------------------------------------------

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
        _persist_task_created(app, task)
        executor.submit(dispatch_parent_task, task["task_id"])
        return jsonify(task_id=task["task_id"], status="CREATED"), 201

    @app.get("/api/tasks")
    def list_tasks() -> tuple[Any, int]:
        task_list = tasks.list()
        return jsonify(tasks=task_list, count=len(task_list)), 200

    @app.get("/api/tasks/history")
    def task_history() -> tuple[Any, int]:
        """Query PostgreSQL for durable historical task records."""
        raw_limit = request.args.get("limit", "50")
        try:
            limit = int(raw_limit)
            if limit <= 0:
                raise ValueError("limit must be positive")
        except ValueError:
            return jsonify(error="limit must be a positive integer"), 400

        tasks_list: list[dict[str, Any]] = []
        try:
            rows = db.session.execute(
                db.select(Task).order_by(Task.created_at.desc()).limit(limit)
            ).scalars().all()
            for row in rows:
                subtasks_list = [
                    {
                        "subtask_id": s.subtask_id,
                        "parent_task_id": s.parent_task_id,
                        "task_type": s.task_type,
                        "start": s.start,
                        "end": s.end,
                        "assigned_node": s.node_id,
                        "status": s.status,
                        "result": s.result,
                        "error": s.error,
                        "created_at": s.created_at.isoformat() if s.created_at else None,
                        "started_at": s.started_at.isoformat() if s.started_at else None,
                        "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                    }
                    for s in row.subtasks
                ]
                tasks_list.append({
                    "task_id": row.task_id,
                    "task_type": row.task_type,
                    "status": row.status,
                    "start": row.start,
                    "end": row.end,
                    "total_subtasks": row.total_subtasks,
                    "completed_subtasks": row.completed_subtasks,
                    "failed_subtasks": row.failed_subtasks,
                    "final_result": row.final_result,
                    "error": row.error,
                    "subtasks": subtasks_list,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "started_at": row.started_at.isoformat() if row.started_at else None,
                    "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                })
        except Exception:
            logger.exception("DB read failed for task history")

        return jsonify(tasks=tasks_list, count=len(tasks_list)), 200

    @app.get("/api/tasks/<task_id>")
    def get_task(task_id: str) -> tuple[Any, int]:
        task = tasks.get(task_id)
        if task is not None:
            return jsonify(task), 200

        # Fall back to DB if task is no longer in memory
        try:
            row = db.session.execute(
                db.select(Task).where(Task.task_id == task_id)
            ).scalar_one_or_none()
            if row is None:
                return jsonify(error="Task not found", task_id=task_id), 404

            subtasks_list = [
                {
                    "subtask_id": s.subtask_id,
                    "parent_task_id": s.parent_task_id,
                    "task_type": s.task_type,
                    "start": s.start,
                    "end": s.end,
                    "assigned_node": s.node_id,
                    "status": s.status,
                    "result": s.result,
                    "error": s.error,
                    "created_at": s.created_at.isoformat() if s.created_at else None,
                    "started_at": s.started_at.isoformat() if s.started_at else None,
                    "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                }
                for s in row.subtasks
            ]

            return jsonify({
                "task_id": row.task_id,
                "task_type": row.task_type,
                "status": row.status,
                "start": row.start,
                "end": row.end,
                "total_subtasks": row.total_subtasks,
                "completed_subtasks": row.completed_subtasks,
                "failed_subtasks": row.failed_subtasks,
                "result": row.final_result,
                "final_result": row.final_result,
                "error": row.error,
                "subtasks": subtasks_list,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "started_at": row.started_at.isoformat() if row.started_at else None,
                "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            }), 200
        except Exception:
            logger.exception("DB read failed for task detail %s", task_id)
            return jsonify(error="Task not found", task_id=task_id), 404

    # -----------------------------------------------------------------------
    # Event endpoints
    # -----------------------------------------------------------------------

    @app.get("/api/events")
    def list_events() -> tuple[Any, int]:
        """Query PostgreSQL for audit event history with optional filters."""
        raw_limit = request.args.get("limit", "50")
        try:
            limit = int(raw_limit)
            if limit <= 0:
                raise ValueError("limit must be positive")
        except ValueError:
            return jsonify(error="limit must be a positive integer"), 400

        event_type = request.args.get("event_type")
        task_id = request.args.get("task_id")
        node_id = request.args.get("node_id")

        events_list: list[dict[str, Any]] = []
        try:
            stmt = db.select(Event)
            if event_type:
                stmt = stmt.where(Event.event_type == event_type)
            if task_id:
                stmt = stmt.where(Event.task_id == task_id)
            if node_id:
                stmt = stmt.where(Event.node_id == node_id)

            stmt = stmt.order_by(Event.timestamp.desc()).limit(limit)
            rows = db.session.execute(stmt).scalars().all()
            events_list = [
                {
                    "id": row.id,
                    "timestamp": row.timestamp.isoformat(),
                    "event_type": row.event_type,
                    "task_id": row.task_id,
                    "subtask_id": row.subtask_id,
                    "node_id": row.node_id,
                    "message": row.message,
                    "severity": row.severity,
                }
                for row in rows
            ]
        except Exception:
            logger.exception("DB read failed for events")

        return jsonify(events=events_list, count=len(events_list)), 200

    if start_monitor:
        def offline_monitor() -> None:
            while True:
                time.sleep(check_interval)
                offline_nodes = nodes.mark_timed_out_nodes_offline()
                _persist_node_offline(app, offline_nodes)

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
