"""Load every raw file under ``data/raw`` into Bronze as immutable evidence.

Usage::

    uv run python -m pipelines.bronze.build_bronze --input data/raw --run-id <pipeline_run_id>

For every source file this stores the complete, unmodified record as JSONB
together with its file name, row/array index, a best-effort natural id (used
only for human debugging, never as a join key), a sha256 checksum of the
record, and the ingestion run that loaded it. No business join,
deduplication, or calculation happens here -- see
``pipelines/silver/build_silver.sql`` for that.

Field names below (``ID_FIELD_CANDIDATES``) assume the conventional
``<entity>_id`` naming described in the milestone README. If the real
Google Drive export uses different field names, only ``source_record_id``
(a debugging aid) is affected -- adjust the candidate list to match once you
have inspected the actual files. See ``data_contract_notes.md`` at the repo
root for the rest of the assumptions this pipeline depends on.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipelines.ingestion.db import apply_sql_file, connect_local, ensure_pipeline_run, record_stage_event

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILES = ("bronze.sql", "silver.sql", "ops.sql", "gold.sql")


@dataclass(frozen=True)
class SourceSpec:
    relative_path: str
    source_system: str
    bronze_table: str
    id_field_candidates: tuple[str, ...]
    file_format: str  # "json" or "csv"


SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec("operational/customers.json", "operational", "bronze.customers", ("customer_id", "id"), "json"),
    SourceSpec("operational/customer_profiles.json", "operational", "bronze.customer_profiles", ("customer_id", "id"), "json"),
    SourceSpec("operational/customer_addresses.json", "operational", "bronze.customer_addresses", ("address_id", "customer_id", "id"), "json"),
    SourceSpec("operational/products.json", "operational", "bronze.products", ("product_id", "id"), "json"),
    SourceSpec("operational/product_categories.json", "operational", "bronze.product_categories", ("product_id", "id"), "json"),
    SourceSpec("operational/stores.json", "operational", "bronze.stores", ("store_id", "id"), "json"),
    SourceSpec("operational/sales_channels.json", "operational", "bronze.sales_channels", ("sales_channel", "channel_id", "id"), "json"),
    SourceSpec("operational/promotions.json", "operational", "bronze.promotions", ("promotion_id", "id"), "json"),
    SourceSpec("operational/orders.json", "operational", "bronze.orders", ("order_id", "id"), "json"),
    SourceSpec("operational/order_items.json", "operational", "bronze.order_items", ("order_item_id", "id"), "json"),
    SourceSpec("operational/order_promotions.json", "operational", "bronze.order_promotions", ("order_id", "id"), "json"),
    SourceSpec("events/payment_events.json", "events", "bronze.payment_events", ("event_id", "payment_event_id", "id"), "json"),
    SourceSpec("events/refund_events.json", "events", "bronze.refund_events", ("event_id", "refund_event_id", "id"), "json"),
    SourceSpec("events/return_events.json", "events", "bronze.return_events", ("event_id", "return_event_id", "id"), "json"),
    SourceSpec("events/support_events.json", "events", "bronze.support_events", ("event_id", "support_event_id", "id"), "json"),
    SourceSpec("events/web_events.json", "events", "bronze.web_events", ("event_id", "web_event_id", "id"), "json"),
    SourceSpec("inventory/inventory_snapshots.csv", "inventory", "bronze.inventory_snapshots", ("product_id", "id"), "csv"),
    SourceSpec("reference/campaign_spend.csv", "reference", "bronze.campaign_spend", ("campaign_id", "id"), "csv"),
    SourceSpec("reference/city_reference.json", "reference", "bronze.city_reference", ("city_id", "city", "city_name", "id"), "json"),
)


def _sha256(record: dict[str, Any]) -> str:
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _record_id(record: dict[str, Any], candidates: tuple[str, ...]) -> str | None:
    for key in candidates:
        value = record.get(key)
        if value is not None:
            return str(value)
    return None


def _load_json_records(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        list_values = [value for value in payload.values() if isinstance(value, list)]
        if len(list_values) == 1:
            return list_values[0]
        return [payload]
    raise ValueError(f"Unsupported JSON shape in {path}")


def _load_csv_records(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_source(connection: Any, input_dir: Path, spec: SourceSpec, pipeline_run_id: str) -> int:
    path = input_dir / spec.relative_path
    if not path.exists():
        print(f"skip {spec.relative_path}: file not found under {input_dir}")
        return 0

    records = _load_json_records(path) if spec.file_format == "json" else _load_csv_records(path)

    for line_number, record in enumerate(records):
        connection.execute(
            f"""
            INSERT INTO {spec.bronze_table} (
                ingestion_run_id, source_system, source_file, source_line_number,
                source_record_id, checksum_sha256, raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ingestion_run_id, source_file, source_line_number)
            DO UPDATE SET
                source_record_id = EXCLUDED.source_record_id,
                checksum_sha256 = EXCLUDED.checksum_sha256,
                raw_payload = EXCLUDED.raw_payload,
                ingested_at = now()
            """,
            (
                pipeline_run_id,
                spec.source_system,
                spec.relative_path,
                line_number,
                _record_id(record, spec.id_field_candidates),
                _sha256(record),
                json.dumps(record, default=str),
            ),
        )

    print(f"bronze <- {spec.relative_path}: {len(records)} record(s) -> {spec.bronze_table}")
    return len(records)


def _ensure_schemas(connection: Any) -> None:
    schemas_dir = PROJECT_ROOT / "database" / "schemas"
    for filename in SCHEMA_FILES:
        apply_sql_file(connection, schemas_dir / filename)


def build_bronze(input_dir: Path, pipeline_run_id: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with connect_local() as connection:
        _ensure_schemas(connection)
        ensure_pipeline_run(connection, pipeline_run_id, "load_bronze")
        for spec in SOURCES:
            counts[spec.relative_path] = load_source(connection, input_dir, spec, pipeline_run_id)
        total = sum(counts.values())
        record_stage_event(connection, pipeline_run_id, "load_bronze", "succeeded", total)
    print(f"Bronze run {pipeline_run_id} stored {total} record(s) across {len(SOURCES)} source(s).")
    return counts


def _default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"manual-{stamp}-{uuid.uuid4().hex[:8]}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(PROJECT_ROOT / "data" / "raw"))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    pipeline_run_id = args.run_id or _default_run_id()
    build_bronze(Path(args.input), pipeline_run_id)


if __name__ == "__main__":
    main()
