"""Stage 4A database-foundation tests using SQLite only for fast isolation."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("IDCP_DATABASE_URI", "sqlite+pysqlite:///:memory:")
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

import app as master  # noqa: E402
from database import database_uri_from_environment, db, initialize_database  # noqa: E402
from models import Node  # noqa: E402


class DatabaseFoundationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            start_monitor=False,
        )

    def test_node_table_is_created_with_required_columns(self) -> None:
        with self.app.app_context():
            columns = {column["name"] for column in inspect(db.engine).get_columns("nodes")}
        self.assertTrue(
            {
                "id",
                "node_id",
                "hostname",
                "platform",
                "status",
                "worker_url",
                "registered_at",
                "last_heartbeat",
                "created_at",
                "updated_at",
            }.issubset(columns)
        )

    def test_node_model_can_be_persisted(self) -> None:
        with self.app.app_context():
            node = Node(
                node_id="node-db-test",
                hostname="test-host",
                platform="test-platform",
                status="ONLINE",
                worker_url="http://node-db-test:5001",
            )
            db.session.add(node)
            db.session.commit()
            stored_node = db.session.get(Node, node.id)
            self.assertIsNotNone(stored_node)
            self.assertEqual(stored_node.node_id, "node-db-test")
            self.assertEqual(stored_node.status, "ONLINE")

    def test_postgres_uri_uses_service_name_and_environment_credentials(self) -> None:
        with patch.dict(
            os.environ,
            {
                "POSTGRES_DB": "idcp_test",
                "POSTGRES_USER": "idcp_user",
                "POSTGRES_PASSWORD": "safe-password",
                "POSTGRES_HOST": "postgres",
                "POSTGRES_PORT": "5432",
            },
            clear=True,
        ):
            uri = database_uri_from_environment()
        self.assertTrue(uri.startswith("postgresql+psycopg://idcp_user:"))
        self.assertIn("@postgres:5432/idcp_test", uri)
        self.assertNotIn("localhost", uri)

    def test_database_initialization_retries_a_transient_connection_error(self) -> None:
        app = master.create_app(
            database_uri="sqlite+pysqlite:///:memory:",
            initialize_db=False,
            start_monitor=False,
        )
        transient_error = OperationalError("SELECT 1", {}, ConnectionError("not ready"))
        with app.app_context(), patch.object(
            db.session, "execute", side_effect=[transient_error, object()]
        ) as execute, patch("database.time.sleep") as sleep, patch.dict(
            os.environ,
            {"DATABASE_RETRY_ATTEMPTS": "2", "DATABASE_RETRY_DELAY_SECONDS": "0"},
        ):
            initialize_database(app)

        self.assertEqual(execute.call_count, 2)
        sleep.assert_called_once_with(0.0)


if __name__ == "__main__":
    unittest.main()
