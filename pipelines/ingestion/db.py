"""Database access for the pipeline, built on top of ``shared.db``.

The pipeline always targets the local PostgreSQL profile
(``ANALYTICS_DB_TARGET=local``), even when the analytics engine defaults to
Neon. Reusing ``shared.db`` keeps one pg8000-based connection implementation
for the whole repository instead of a second driver stack.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.db import apply_sql_file, connect, dsn_from_environment

__all__ = [
    "apply_sql_file",
    "apply_sql_template",
    "connect",
    "connect_local",
    "dsn_from_environment",
    "ensure_pipeline_run",
    "mark_pipeline_failed",
    "record_stage_event",
]


def connect_local() -> Any:
    """Connect to the local PostgreSQL profile used by the pipeline."""

    return connect("local")


def apply_sql_template(connection: Any, path: Path, **params: str) -> None:
    """Run a ``.sql`` file after substituting ``{{TOKEN}}`` placeholders.

    ``build_silver.sql`` and ``build_gold.sql`` are plain SQL files (not
    Python), so a pipeline_run_id can't be bound as a query parameter the
    usual way. Callers pass it as text and this does a literal-safe
    substitution before the script reaches the server.
    """

    text = path.read_text(encoding="utf-8")
    for key, value in params.items():
        token = "{{" + key.upper() + "}}"
        safe_value = str(value).replace("'", "''")
        text = text.replace(token, safe_value)
    connection.execute(text)


def ensure_pipeline_run(connection: Any, pipeline_run_id: str, stage: str, triggered_by: str = "airflow") -> None:
    """Create/refresh the ops.pipeline_runs row for a run, then mark ``stage``.

    Every stage script calls this before doing work, so any of the seven
    Airflow tasks can be the first to touch PostgreSQL for a given run --
    the DAG does not depend on task ordering to bootstrap ops metadata.
    """

    connection.execute(
        """
        INSERT INTO ops.pipeline_runs (pipeline_run_id, triggered_by, current_stage)
        VALUES (%s, %s, %s)
        ON CONFLICT (pipeline_run_id) DO UPDATE SET current_stage = EXCLUDED.current_stage
        """,
        (pipeline_run_id, triggered_by, stage),
    )


def record_stage_event(
    connection: Any,
    pipeline_run_id: str,
    stage: str,
    status: str,
    row_count: int | None = None,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO ops.stage_events (pipeline_run_id, stage, status, row_count, error_message)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (pipeline_run_id, stage, status, row_count, error_message),
    )


def mark_pipeline_failed(connection: Any, pipeline_run_id: str, message: str) -> None:
    connection.execute(
        """
        UPDATE ops.pipeline_runs
        SET status = 'failed', finished_at = now(), error_message = %s
        WHERE pipeline_run_id = %s
        """,
        (message, pipeline_run_id),
    )
