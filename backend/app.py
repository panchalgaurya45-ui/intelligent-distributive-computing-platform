"""IDCP Stage 1 master service.

The master keeps the live state of registered worker nodes in memory.  A future
stage can replace ``NodeRegistry`` with a persistent repository without
changing the HTTP API used by node agents.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, request


LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("idcp.master")


def utc_now() -> str:
    """Return an ISO 8601 UTC timestamp suitable for JSON responses."""
    return datetime.now(timezone.utc).isoformat()


class NodeRegistry:
    """Thread-safe, in-memory registry for Stage 1 worker node state."""

    def __init__(self, heartbeat_timeout_seconds: float) -> None:
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._nodes: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def register(self, node_id: str, hostname: str, platform: str) -> tuple[dict[str, Any], bool]:
        """Register or refresh a node and return a copy plus whether it is new."""
        with self._lock:
            existing = self._nodes.get(node_id)
            now = time.monotonic()
            if existing is None:
                node = {
                    "node_id": node_id,
                    "hostname": hostname,
                    "platform": platform,
                    "status": "ONLINE",
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


def require_json_object() -> tuple[dict[str, Any] | None, tuple[Any, int] | None]:
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return None, (jsonify(error="Request body must be a JSON object"), 400)
    return payload, None


def create_app(
    heartbeat_timeout_seconds: float | None = None,
    offline_check_interval_seconds: float | None = None,
) -> Flask:
    """Application factory, also used by tests and future deployment servers."""
    timeout = heartbeat_timeout_seconds or float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))
    check_interval = offline_check_interval_seconds or float(os.getenv("OFFLINE_CHECK_INTERVAL_SECONDS", "2"))
    if timeout <= 0 or check_interval <= 0:
        raise ValueError("Heartbeat timeout and offline check interval must be positive")

    app = Flask(__name__)
    registry = NodeRegistry(timeout)
    app.config["NODE_REGISTRY"] = registry
    app.config["HEARTBEAT_TIMEOUT_SECONDS"] = timeout

    @app.post("/api/nodes/register")
    def register_node() -> tuple[Any, int]:
        payload, error = require_json_object()
        if error:
            return error
        try:
            node, is_new = registry.register(
                required_text(payload, "node_id"),
                required_text(payload, "hostname"),
                required_text(payload, "platform"),
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

        node = registry.heartbeat(
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
        nodes = registry.list_nodes()
        return jsonify(nodes=nodes, count=len(nodes)), 200

    @app.get("/api/health")
    def health() -> tuple[Any, int]:
        total_nodes, online_nodes = registry.counts()
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

    def offline_monitor() -> None:
        while True:
            time.sleep(check_interval)
            registry.mark_timed_out_nodes_offline()

    monitor = threading.Thread(target=offline_monitor, name="offline-monitor", daemon=True)
    monitor.start()
    logger.info(
        "IDCP master initialized (heartbeat timeout: %ss, check interval: %ss)",
        timeout,
        check_interval,
    )
    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    logger.info("Starting IDCP master on 0.0.0.0:%s", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
