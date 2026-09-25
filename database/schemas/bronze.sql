-- Bronze layer: immutable source evidence.
--
-- One table per raw source file. Every table shares the same lineage
-- columns so `pipelines/bronze/build_bronze.py` can load all nineteen
-- sources with one code path. Bronze never joins, dedups, or computes
-- metrics -- invalid and duplicate records are kept as-is; Silver decides
-- what to keep.
--
-- Idempotency: reloading the same pipeline_run_id upserts on
-- (ingestion_run_id, source_file, source_line_number) instead of
-- appending again, so a retried Bronze task never doubles the row count
-- for that run. Loading a *new* run intentionally appends a fresh copy,
-- which is how source duplicates and late-arriving files stay visible
-- across runs for Silver to reconcile.

CREATE SCHEMA IF NOT EXISTS bronze;

-- template (documentation only, not executed):
-- CREATE TABLE bronze.<entity> (
--     bronze_id           BIGSERIAL PRIMARY KEY,
--     ingestion_run_id    TEXT NOT NULL,
--     source_system       TEXT NOT NULL,
--     source_file         TEXT NOT NULL,
--     source_line_number  INTEGER NOT NULL,
--     source_record_id    TEXT,
--     checksum_sha256     TEXT NOT NULL,
--     raw_payload         JSONB NOT NULL,
--     ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
--     UNIQUE (ingestion_run_id, source_file, source_line_number)
-- );

CREATE TABLE IF NOT EXISTS bronze.customers (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.customer_profiles (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.customer_addresses (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.products (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.product_categories (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.stores (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.sales_channels (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.promotions (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.orders (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.order_items (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.order_promotions (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.payment_events (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.refund_events (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.return_events (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.support_events (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.web_events (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.inventory_snapshots (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.campaign_spend (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS bronze.city_reference (
    bronze_id           BIGSERIAL PRIMARY KEY,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    source_file         TEXT NOT NULL,
    source_line_number  INTEGER NOT NULL,
    source_record_id    TEXT,
    checksum_sha256     TEXT NOT NULL,
    raw_payload         JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ingestion_run_id, source_file, source_line_number)
);

CREATE INDEX IF NOT EXISTS ix_bronze_customers_record_id ON bronze.customers (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_customer_profiles_record_id ON bronze.customer_profiles (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_customer_addresses_record_id ON bronze.customer_addresses (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_products_record_id ON bronze.products (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_product_categories_record_id ON bronze.product_categories (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_stores_record_id ON bronze.stores (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_sales_channels_record_id ON bronze.sales_channels (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_promotions_record_id ON bronze.promotions (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_orders_record_id ON bronze.orders (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_order_items_record_id ON bronze.order_items (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_order_promotions_record_id ON bronze.order_promotions (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_payment_events_record_id ON bronze.payment_events (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_refund_events_record_id ON bronze.refund_events (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_return_events_record_id ON bronze.return_events (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_support_events_record_id ON bronze.support_events (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_web_events_record_id ON bronze.web_events (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_inventory_snapshots_record_id ON bronze.inventory_snapshots (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_campaign_spend_record_id ON bronze.campaign_spend (source_record_id);
CREATE INDEX IF NOT EXISTS ix_bronze_city_reference_record_id ON bronze.city_reference (source_record_id);
