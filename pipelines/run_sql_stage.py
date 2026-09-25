"""Generic CLI wrapper that runs a Silver/Gold SQL transform as one Airflow
task and records the outcome in ``ops``.

Usage::

    python -m pipelines.run_sql_stage --stage build_silver \\
        --sql pipelines/silver/build_silver.sql --run-id {{ run_id }}

Kept generic (rather than one wrapper per layer) because build_silver.sql
and build_gold.sql are plain SQL, not Python modules -- this is the only
piece of Python either of them needs.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipelines.ingestion.db import (
    apply_sql_template,
    connect_local,
    ensure_pipeline_run,
    mark_pipeline_failed,
    record_stage_event,
)


def run_sql_stage(stage: str, sql_path: Path, pipeline_run_id: str) -> None:
    with connect_local() as connection:
        ensure_pipeline_run(connection, pipeline_run_id, stage)
        try:
            apply_sql_template(connection, sql_path, pipeline_run_id=pipeline_run_id)
        except Exception as error:
            record_stage_event(connection, pipeline_run_id, stage, "failed", error_message=str(error))
            mark_pipeline_failed(connection, pipeline_run_id, f"{stage} failed: {error}")
            raise
        record_stage_event(connection, pipeline_run_id, stage, "succeeded")
        if stage == "build_gold":
            connection.execute(
                "UPDATE ops.pipeline_runs SET status = 'succeeded', finished_at = now() WHERE pipeline_run_id = %s",
                (pipeline_run_id,),
            )
    print(f"{stage} applied {sql_path} for run {pipeline_run_id}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    try:
        run_sql_stage(args.stage, Path(args.sql), args.run_id)
    except Exception as error:  # noqa: BLE001 - surface as a failed Airflow task
        print(f"{args.stage} failed: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
