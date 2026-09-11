"""Reusable IDCP worker node agent for Stage 1 monitoring and Stage 2 work."""

from __future__ import annotations

import json
import logging
import os
import platform as platform_module
import socket
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

import psutil
import requests


LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("idcp.node_agent")

TASK_TYPE_SUM_RANGE = "sum_range"
MAX_REQUEST_BODY_BYTES = 64 * 1024


class TaskValidationError(ValueError):
    """An expected bad task request that can safely be returned to callers."""


def required_text(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise TaskValidationError(f"{field_name} is required and must be a non-empty string")
    return value.strip()


def sum_range(start: int, end: int) -> int:
    """Perform the actual Stage 2 computation using Python integer arithmetic."""
    return sum(range(start, end + 1))


def validate_task(payload: dict[str, Any]) -> tuple[str, str, int, int]:
    """Validate the small, explicit Stage 2 worker task contract."""
    task_id = required_text(payload, "task_id")
    task_type = required_text(payload, "task_type")
    if task_type != TASK_TYPE_SUM_RANGE:
        raise TaskValidationError(f"Unsupported task_type: {task_type}")

    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        raise TaskValidationError("parameters is required and must be an object")
    start = parameters.get("start")
    end = parameters.get("end")
    if isinstance(start, bool) or not isinstance(start, int):
        raise TaskValidationError("parameters.start must be an integer")
    if isinstance(end, bool) or not isinstance(end, int):
        raise TaskValidationError("parameters.end must be an integer")
    if start > end:
        raise TaskValidationError("parameters.start must be less than or equal to parameters.end")
    return task_id, task_type, start, end


class TaskExecutionHandler(BaseHTTPRequestHandler):
    """Tiny JSON API exposed inside the Docker network by every node agent."""

    server: ThreadingHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("Worker API %s - %s", self.address_string(), format % args)

    def _send_json(self, status_code: int, body: dict[str, Any]) -> None:
        encoded_body = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded_body)))
        self.end_headers()
        self.wfile.write(encoded_body)

    def _read_json_object(self) -> dict[str, Any]:
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise TaskValidationError("Content-Length must be valid") from exc
        if content_length <= 0:
            raise TaskValidationError("Request body must be a JSON object")
        if content_length > MAX_REQUEST_BODY_BYTES:
            raise TaskValidationError("Request body is too large")
        try:
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TaskValidationError("Request body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise TaskValidationError("Request body must be a JSON object")
        return payload

    def do_POST(self) -> None:  # noqa: N802 - required standard-library handler name
        if urlparse(self.path).path != "/api/tasks/execute":
            self._send_json(404, {"error": "Endpoint not found"})
            return
        try:
            payload = self._read_json_object()
            agent = getattr(self.server, "node_agent")
            response = agent.execute_task(payload)
            self._send_json(200, response)
        except TaskValidationError as exc:
            self._send_json(400, {"status": "FAILED", "error": str(exc)})
        except Exception:
            logger.exception("Unexpected task execution error")
            self._send_json(500, {"status": "FAILED", "error": "Task execution failed"})


class NodeAgent:
    """Registers one node, reports metrics, and executes tasks sent by master."""

    def __init__(
        self,
        node_id: str,
        master_url: str,
        worker_url: str,
        worker_bind_host: str,
        worker_port: int,
        heartbeat_interval_seconds: float,
        request_timeout_seconds: float,
    ) -> None:
        self.node_id = node_id
        self.master_url = master_url.rstrip("/")
        self.worker_url = worker_url.rstrip("/")
        self.worker_bind_host = worker_bind_host
        self.worker_port = worker_port
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.hostname = socket.gethostname()
        self.platform = platform_module.platform()
        self.registered = False
        self.session = requests.Session()
        self._worker_server: ThreadingHTTPServer | None = None

    def registration_payload(self) -> dict[str, str]:
        return {
            "node_id": self.node_id,
            "hostname": self.hostname,
            "platform": self.platform,
            "worker_url": self.worker_url,
        }

    def collect_metrics(self) -> dict[str, Any]:
        """Read real metrics from this node's operating-system/container view."""
        memory = psutil.virtual_memory()
        return {
            "node_id": self.node_id,
            "hostname": self.hostname,
            "platform": self.platform,
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": memory.percent,
            "available_memory": memory.available,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def execute_task(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Validate and execute a real task; this runs in a worker API thread."""
        task_id, task_type, start, end = validate_task(payload)
        logger.info("Task %s received: %s (%s..%s)", task_id, task_type, start, end)
        result = sum_range(start, end)
        logger.info("Task %s completed with result %s", task_id, result)
        return {"task_id": task_id, "status": "COMPLETED", "result": result}

    def start_worker_server(self) -> None:
        """Start the task endpoint without blocking the heartbeat loop."""
        if self._worker_server is not None:
            return
        server = ThreadingHTTPServer((self.worker_bind_host, self.worker_port), TaskExecutionHandler)
        server.daemon_threads = True
        setattr(server, "node_agent", self)
        self._worker_server = server
        thread = threading.Thread(target=server.serve_forever, name="task-api", daemon=True)
        thread.start()
        logger.info(
            "Worker task API listening on %s:%s (advertised as %s)",
            self.worker_bind_host,
            self.worker_port,
            self.worker_url,
        )

    def _post(self, path: str, payload: dict[str, Any]) -> requests.Response:
        return self.session.post(
            f"{self.master_url}{path}",
            json=payload,
            timeout=self.request_timeout_seconds,
        )

    def register(self) -> bool:
        try:
            response = self._post("/api/nodes/register", self.registration_payload())
            response.raise_for_status()
        except requests.RequestException as exc:
            self.registered = False
            logger.warning("Registration failed; will retry: %s", exc)
            return False
        self.registered = True
        logger.info("Registered with master at %s", self.master_url)
        return True

    def send_heartbeat(self) -> bool:
        try:
            response = self._post("/api/nodes/heartbeat", self.collect_metrics())
            if response.status_code == 404:
                # A master restart loses its in-memory Stage 1/2 registries.
                self.registered = False
                logger.warning("Master no longer knows this node; re-registering")
                return False
            response.raise_for_status()
        except requests.RequestException as exc:
            self.registered = False
            logger.warning("Heartbeat failed; will retry registration: %s", exc)
            return False
        logger.debug("Heartbeat sent successfully")
        return True

    def run_forever(self) -> None:
        self.start_worker_server()
        logger.info(
            "Starting node agent for %s; master=%s, interval=%ss",
            self.node_id,
            self.master_url,
            self.heartbeat_interval_seconds,
        )
        while True:
            if not self.registered:
                self.register()
            if self.registered:
                self.send_heartbeat()
            time.sleep(self.heartbeat_interval_seconds)


def environment_positive_float(name: str, default: str) -> float:
    value = float(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def environment_port(name: str, default: str) -> int:
    value = int(os.getenv(name, default))
    if not 1 <= value <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535")
    return value


def main() -> None:
    node_id = os.getenv("NODE_ID", "").strip()
    if not node_id:
        raise ValueError("NODE_ID environment variable is required")
    worker_port = environment_port("WORKER_PORT", "5001")
    agent = NodeAgent(
        node_id=node_id,
        master_url=os.getenv("MASTER_URL", "http://master:5000"),
        worker_url=os.getenv("WORKER_URL", f"http://{node_id}:{worker_port}"),
        worker_bind_host=os.getenv("WORKER_BIND_HOST", "0.0.0.0"),
        worker_port=worker_port,
        heartbeat_interval_seconds=environment_positive_float("HEARTBEAT_INTERVAL_SECONDS", "5"),
        request_timeout_seconds=environment_positive_float("REQUEST_TIMEOUT_SECONDS", "3"),
    )
    agent.run_forever()


if __name__ == "__main__":
    main()
