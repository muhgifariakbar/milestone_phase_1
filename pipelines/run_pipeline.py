"""Local, non-Airflow entrypoint that runs the whole pipeline end to end.

Usage::

    uv run python -m pipelines.run_pipeline \\
      --input data/raw --bronze student --silver student --gold student

``dags/dag.py`` runs the same three stages as separate Airflow tasks (so
each one gets its own retry/log); this module is the one-shot local
equivalent used for development. Every stage function used here already
records its own ``ops.stage_events``/``ops.pipeline_runs`` rows, so this is
just sequencing and exit-code plumbing, not a second source of truth for
run status.

``--bronze/--silver/--gold`` accept ``student`` (this package) or
``reference``. ``reference`` points at the instructor solution under
``solution/``, which is deliberately not part of the participant package --
selecting it here fails with a clear message rather than a stack trace.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pipelines.bronze.build_bronze import build_bronze
from pipelines.run_sql_stage import run_sql_stage

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_SQL = {"student": PROJECT_ROOT / "pipelines" / "silver" / "build_silver.sql"}
GOLD_SQL = {"student": PROJECT_ROOT / "pipelines" / "gold" / "build_gold.sql"}


def _resolve_sql(kind: str, choice: str, table: dict[str, Path]) -> Path:
    if choice not in table:
        sys.exit(
            f"--{kind} reference is not part of the participant package; "
            f"the instructor build lives under solution/ and is not distributed here."
        )
    return table[choice]


def _new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"local-{stamp}-{uuid.uuid4().hex[:8]}"


def run_pipeline(input_dir: Path, bronze: str, silver: str, gold: str, pipeline_run_id: str | None) -> bool:
    if bronze != "student":
        sys.exit("--bronze reference is not part of the participant package.")
    silver_sql = _resolve_sql("silver", silver, SILVER_SQL)
    gold_sql = _resolve_sql("gold", gold, GOLD_SQL)

    pipeline_run_id = pipeline_run_id or _new_run_id()
    print(f"Pipeline run: {pipeline_run_id}")

    build_bronze(input_dir, pipeline_run_id)

    try:
        run_sql_stage("build_silver", silver_sql, pipeline_run_id)
    except Exception as error:
        print(f"Pipeline run {pipeline_run_id} failed: build_silver failed: {error}")
        return False

    try:
        run_sql_stage("build_gold", gold_sql, pipeline_run_id)
    except Exception as error:
        print(f"Pipeline run {pipeline_run_id} failed: build_gold failed: {error}")
        return False

    print(f"Pipeline run {pipeline_run_id} succeeded.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(PROJECT_ROOT / "data" / "raw"))
    parser.add_argument("--bronze", default="student", choices=["student", "reference"])
    parser.add_argument("--silver", default="student", choices=["student", "reference"])
    parser.add_argument("--gold", default="student", choices=["student", "reference"])
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    ok = run_pipeline(Path(args.input), args.bronze, args.silver, args.gold, args.run_id)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
