"""Database configuration and safe startup initialization for IDCP Stage 4A."""

from __future__ import annotations

import logging
import os
import time

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import URL, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase


logger = logging.getLogger("idcp.database")


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for all persistent IDCP models."""


db = SQLAlchemy(model_class=Base)


def database_uri_from_environment() -> str:
    """Build a PostgreSQL URI without embedding credentials in application code.

    ``IDCP_DATABASE_URI`` is an optional test/local override. Docker production
    configuration supplies POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD,
    while the host deliberately defaults to the Docker service name ``postgres``.
    """
    override_uri = os.getenv("IDCP_DATABASE_URI")
    if override_uri:
        return override_uri

    required = ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required database environment variable(s): {', '.join(missing)}")

    return URL.create(
        drivername="postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    ).render_as_string(hide_password=False)


def configure_database(app: Flask, database_uri: str | None = None) -> None:
    """Configure and attach the Flask-SQLAlchemy extension before first use."""
    app.config["SQLALCHEMY_DATABASE_URI"] = database_uri or database_uri_from_environment()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True}
    db.init_app(app)


def initialize_database(app: Flask) -> None:
    """Wait for PostgreSQL, then create the Stage 4A schema safely.

    PostgreSQL can accept its container process before it accepts connections.
    Retrying a bounded, configurable number of times gives it time to finish
    initialization while still surfacing a genuine configuration problem.
    """
    attempts = int(os.getenv("DATABASE_RETRY_ATTEMPTS", "30"))
    retry_delay_seconds = float(os.getenv("DATABASE_RETRY_DELAY_SECONDS", "2"))
    if attempts <= 0 or retry_delay_seconds < 0:
        raise ValueError("Database retry settings must be positive (delay may be zero)")

    with app.app_context():
        for attempt in range(1, attempts + 1):
            try:
                db.session.execute(text("SELECT 1"))
                db.create_all()
                logger.info("Database connection established and schema initialized")
                return
            except OperationalError as exc:
                db.session.rollback()
                if attempt == attempts:
                    logger.error("Database was unavailable after %s attempts", attempts)
                    raise RuntimeError("Database connection could not be established") from exc
                logger.warning(
                    "Database connection attempt %s/%s failed; retrying in %ss",
                    attempt,
                    attempts,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
