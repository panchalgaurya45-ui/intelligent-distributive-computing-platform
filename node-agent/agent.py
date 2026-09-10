"""Reusable IDCP Stage 1 node agent.

The same program runs in every worker container.  It intentionally talks to a
configurable master URL, allowing later deployment on a remote machine without
altering this agent's collection or retry logic.
"""

from __future__ import annotations

import logging
import os
import platform as platform_module
import socket
import time
from datetime import datetime, timezone
from typing import Any

import psutil
import requests


LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format=LOG_FORMAT)
logger = logging.getLogger("idcp.node_agent")


class NodeAgent:
    """Registers one node and keeps its current container metrics flowing."""

    def __init__(
        self,
        node_id: str,
        master_url: str,
        heartbeat_interval_seconds: float,
        request_timeout_seconds: float,
    ) -> None:
        self.node_id = node_id
        self.master_url = master_url.rstrip("/")
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.hostname = socket.gethostname()
        self.platform = platform_module.platform()
        self.registered = False
        self.session = requests.Session()

    def registration_payload(self) -> dict[str, str]:
        return {
            "node_id": self.node_id,
            "hostname": self.hostname,
            "platform": self.platform,
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
                # A master restart loses its Stage 1 in-memory registry.
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


def main() -> None:
    node_id = os.getenv("NODE_ID", "").strip()
    if not node_id:
        raise ValueError("NODE_ID environment variable is required")
    agent = NodeAgent(
        node_id=node_id,
        master_url=os.getenv("MASTER_URL", "http://master:5000"),
        heartbeat_interval_seconds=environment_positive_float("HEARTBEAT_INTERVAL_SECONDS", "5"),
        request_timeout_seconds=environment_positive_float("REQUEST_TIMEOUT_SECONDS", "3"),
    )
    agent.run_forever()


if __name__ == "__main__":
    main()
