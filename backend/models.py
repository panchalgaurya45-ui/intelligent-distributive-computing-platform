"""Persistent SQLAlchemy models introduced in IDCP Stage 4A/4B."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import db


def utc_now() -> datetime:
    """Return a timezone-aware timestamp for application-managed columns."""
    return datetime.now(timezone.utc)


class Node(db.Model):
    """Persistent node identity and liveness fields introduced in Stage 4A."""

    __tablename__ = "nodes"

    id: Mapped[int] = mapped_column(primary_key=True)
    node_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    platform: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    worker_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Stage 4B: one Node has many NodeMetric rows.
    metrics: Mapped[list[NodeMetric]] = relationship(
        "NodeMetric", back_populates="node", passive_deletes=True
    )


class NodeMetric(db.Model):
    """One persisted heartbeat sample per worker node, introduced in Stage 4B.

    A new row is inserted for every valid heartbeat so that the full
    history of CPU and memory readings is preserved.  Queries use the
    ``(node_id, timestamp)`` composite index for time-range and
    latest-record look-ups.
    """

    __tablename__ = "node_metrics"

    __table_args__ = (
        # Composite index for "latest N metrics for node X" queries.
        Index("ix_node_metrics_node_id_timestamp", "node_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    node_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("nodes.node_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    cpu_percent: Mapped[float] = mapped_column(Float, nullable=False)
    memory_percent: Mapped[float] = mapped_column(Float, nullable=False)
    # Bytes; BigInteger prevents overflow on machines with > 2 GiB available RAM.
    available_memory: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Nullable: latency is not yet reliably measurable from the master side.
    heartbeat_latency: Mapped[float | None] = mapped_column(Float, nullable=True)

    node: Mapped[Node] = relationship("Node", back_populates="metrics")


class Task(db.Model):
    """Persistent execution history for workload parent tasks, introduced in Stage 4C."""

    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    start: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    total_subtasks: Mapped[int] = mapped_column(default=0, nullable=False)
    completed_subtasks: Mapped[int] = mapped_column(default=0, nullable=False)
    failed_subtasks: Mapped[int] = mapped_column(default=0, nullable=False)
    final_result: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True, nullable=False, default=utc_now
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    subtasks: Mapped[list[Subtask]] = relationship(
        "Subtask", back_populates="task", cascade="all, delete-orphan", passive_deletes=True
    )
    events: Mapped[list[Event]] = relationship("Event", back_populates="task")


class Subtask(db.Model):
    """Persistent execution history for split subtasks, introduced in Stage 4C."""

    __tablename__ = "subtasks"

    id: Mapped[int] = mapped_column(primary_key=True)
    subtask_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    parent_task_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("tasks.task_id", ondelete="CASCADE"), index=True, nullable=False
    )
    node_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("nodes.node_id", ondelete="SET NULL"), index=True, nullable=True
    )
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    start: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    result: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    task: Mapped[Task] = relationship("Task", back_populates="subtasks")
    node: Mapped[Node | None] = relationship("Node")


class Event(db.Model):
    """Chronological event audit log, introduced in Stage 4C."""

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), index=True, nullable=False, default=utc_now
    )
    event_type: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    task_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("tasks.task_id", ondelete="SET NULL"), index=True, nullable=True
    )
    subtask_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("subtasks.subtask_id", ondelete="SET NULL"), index=True, nullable=True
    )
    node_id: Mapped[str | None] = mapped_column(
        String(128), ForeignKey("nodes.node_id", ondelete="SET NULL"), index=True, nullable=True
    )
    message: Mapped[str] = mapped_column(String(1024), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="INFO")

    task: Mapped[Task | None] = relationship("Task", back_populates="events")
    subtask: Mapped[Subtask | None] = relationship("Subtask")
    node: Mapped[Node | None] = relationship("Node")
