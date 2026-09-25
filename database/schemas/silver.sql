-- Silver layer: typed, normalized, validated entities with lineage back to
-- Bronze. Built by pipelines/silver/build_silver.sql.
--
-- Field names here match the real Google Drive export (confirmed by
-- inspecting the downloaded files), not a guess -- see
-- data_contract_notes.md for what each source file actually looks like.
--
-- Conventions used across every table:
--   * source_bronze_id       -- the winning Bronze row for this record.
--   * duplicate_bronze_ids   -- other Bronze rows collapsed into it by
--                               deterministic dedup (kept for audit, never
--                               used by Gold).
--   * pipeline_run_id        -- the run that (re)built the row.
--   * silver_loaded_at       -- when this row was written.
--   * timestamps are always UTC (see build_silver.sql for the normalization).
--
-- customer_profiles, customer_addresses and product_categories are
-- slowly-changing (Type 2). The source already ships explicit
-- valid_from_utc/valid_to_utc per version, so Silver keeps that window
-- as-is rather than re-deriving it.
--
-- payment_events, refund_events, return_events and support_events arrive
-- as a raw event log (event_id, event_type, occurred_at_utc, a nested
-- payload) where several events describe the lifecycle of one underlying
-- entity (one payment_id goes through AUTHORIZED -> CAPTURED, one
-- ticket_id goes CREATED -> CLOSED, ...). Silver reconstructs one row per
-- entity holding its resolved terminal status, per the README's "event
-- identity resolution" / "reconstruct state" requirement -- it does not
-- keep one row per raw event. web_events is the exception: ADD_TO_CART /
-- PAGE_VIEW / PURCHASE are independent behavioral signals, not states of
-- one object, so it stays a flat event log.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id           TEXT PRIMARY KEY,
    city_id               TEXT,
    customer_segment      TEXT,
    created_at            TIMESTAMPTZ,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.customer_profiles (
    profile_version_id  BIGSERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    city_id             TEXT,
    customer_segment    TEXT,
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_to            TIMESTAMPTZ,
    is_current          BOOLEAN NOT NULL,
    source_bronze_id    BIGINT NOT NULL,
    pipeline_run_id     TEXT NOT NULL,
    silver_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, valid_from)
);

CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id          TEXT PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    city_id             TEXT,
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_to            TIMESTAMPTZ,
    is_current          BOOLEAN NOT NULL,
    source_bronze_id    BIGINT NOT NULL,
    pipeline_run_id     TEXT NOT NULL,
    silver_loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id          TEXT PRIMARY KEY,
    product_name        TEXT,
    sku                 TEXT,
    unit_price          NUMERIC(14, 2),
    source_bronze_id     BIGINT NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    silver_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.product_categories (
    category_version_id  BIGSERIAL PRIMARY KEY,
    product_id           TEXT NOT NULL,
    category_id          TEXT,
    category_name        TEXT NOT NULL,
    valid_from            TIMESTAMPTZ NOT NULL,
    valid_to              TIMESTAMPTZ,
    is_current            BOOLEAN NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id, valid_from)
);

CREATE TABLE IF NOT EXISTS silver.stores (
    store_id             TEXT PRIMARY KEY,
    store_name           TEXT,
    city_id              TEXT,
    location_type        TEXT,
    source_bronze_id     BIGINT NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    silver_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.sales_channels (
    channel_id           TEXT PRIMARY KEY,
    channel_name         TEXT,
    source_bronze_id     BIGINT NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    silver_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id         TEXT PRIMARY KEY,
    promotion_code       TEXT,
    promotion_type       TEXT,
    discount_rate        NUMERIC(14, 4),
    starts_at            DATE,
    ends_at              DATE,
    source_bronze_id     BIGINT NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    silver_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id              TEXT PRIMARY KEY,
    customer_id           TEXT,
    store_id              TEXT,
    sales_channel         TEXT NOT NULL,
    order_ts              TIMESTAMPTZ NOT NULL,
    order_date            DATE NOT NULL,
    order_status          TEXT NOT NULL,
    shipping_revenue      NUMERIC(14, 2) NOT NULL DEFAULT 0,
    currency              TEXT NOT NULL DEFAULT 'USD',
    has_valid_customer    BOOLEAN NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id         TEXT PRIMARY KEY,
    order_id              TEXT NOT NULL,
    product_id            TEXT,
    quantity              INTEGER NOT NULL,
    unit_price            NUMERIC(14, 2) NOT NULL,
    item_discount_amount  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    line_amount           NUMERIC(14, 2) NOT NULL,
    has_valid_product     BOOLEAN NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_id             TEXT NOT NULL,
    promotion_id         TEXT NOT NULL,
    discount_amount      NUMERIC(14, 2) NOT NULL,
    source_bronze_id     BIGINT NOT NULL,
    pipeline_run_id      TEXT NOT NULL,
    silver_loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, promotion_id)
);

-- One row per payment_id (not per raw event): AUTHORIZED -> CAPTURED /
-- FAILED collapsed to the terminal status.
CREATE TABLE IF NOT EXISTS silver.payment_events (
    payment_id            TEXT PRIMARY KEY,
    order_id              TEXT NOT NULL,
    event_ts              TIMESTAMPTZ NOT NULL,
    amount                NUMERIC(14, 2) NOT NULL,
    status                TEXT NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per refund_id: ISSUED -> COMPLETED collapsed to terminal status.
CREATE TABLE IF NOT EXISTS silver.refund_events (
    refund_id             TEXT PRIMARY KEY,
    order_id              TEXT NOT NULL,
    event_ts              TIMESTAMPTZ NOT NULL,
    amount                NUMERIC(14, 2) NOT NULL,
    status                TEXT NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per return_id: REQUESTED -> RECEIVED -> CLOSED collapsed to
-- terminal status. The source has no line-item/quantity detail for
-- returns (see data_contract_notes.md).
CREATE TABLE IF NOT EXISTS silver.return_events (
    return_id             TEXT PRIMARY KEY,
    order_id              TEXT NOT NULL,
    event_ts              TIMESTAMPTZ NOT NULL,
    status                TEXT NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per ticket_id: CREATED -> CLOSED collapsed to terminal status.
CREATE TABLE IF NOT EXISTS silver.support_events (
    ticket_id             TEXT PRIMARY KEY,
    customer_id           TEXT,
    order_id              TEXT,
    event_ts              TIMESTAMPTZ NOT NULL,
    reason                TEXT,
    status                TEXT NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Flat event log (not collapsed): one row per web event.
CREATE TABLE IF NOT EXISTS silver.web_events (
    web_event_id          TEXT PRIMARY KEY,
    customer_id           TEXT,
    session_id            TEXT,
    order_id              TEXT,
    event_ts              TIMESTAMPTZ NOT NULL,
    event_type            TEXT,
    sales_channel         TEXT,
    campaign_id           TEXT,
    source_bronze_id      BIGINT NOT NULL,
    duplicate_bronze_ids  BIGINT[] NOT NULL DEFAULT '{}',
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    product_id           TEXT NOT NULL,
    store_id              TEXT NOT NULL,
    snapshot_date         DATE NOT NULL,
    available_quantity    INTEGER NOT NULL,
    reserved_quantity     INTEGER NOT NULL DEFAULT 0,
    source_bronze_id      BIGINT NOT NULL,
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, store_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    campaign_id           TEXT NOT NULL,
    sales_channel         TEXT NOT NULL,
    spend_date            DATE NOT NULL,
    spend_amount          NUMERIC(14, 2) NOT NULL,
    source_bronze_id      BIGINT NOT NULL,
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign_id, sales_channel, spend_date)
);

CREATE TABLE IF NOT EXISTS silver.city_reference (
    city_id               TEXT PRIMARY KEY,
    city_name             TEXT,
    country               TEXT,
    source_bronze_id      BIGINT NOT NULL,
    pipeline_run_id       TEXT NOT NULL,
    silver_loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every record Silver refuses to promote lands here with the reason,
-- instead of being silently dropped.
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    id                BIGSERIAL PRIMARY KEY,
    source_table      TEXT NOT NULL,
    bronze_id         BIGINT,
    natural_key       TEXT,
    rejection_reason  TEXT NOT NULL,
    raw_payload       JSONB,
    pipeline_run_id   TEXT NOT NULL,
    rejected_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_silver_orders_customer ON silver.orders (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_order_items_order ON silver.order_items (order_id);
CREATE INDEX IF NOT EXISTS ix_silver_payment_events_order ON silver.payment_events (order_id);
CREATE INDEX IF NOT EXISTS ix_silver_refund_events_order ON silver.refund_events (order_id);
CREATE INDEX IF NOT EXISTS ix_silver_return_events_order ON silver.return_events (order_id);
CREATE INDEX IF NOT EXISTS ix_silver_support_events_customer ON silver.support_events (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_web_events_customer ON silver.web_events (customer_id);
CREATE INDEX IF NOT EXISTS ix_silver_web_events_order ON silver.web_events (order_id);
CREATE INDEX IF NOT EXISTS ix_silver_customer_profiles_customer ON silver.customer_profiles (customer_id, is_current);
CREATE INDEX IF NOT EXISTS ix_silver_product_categories_product ON silver.product_categories (product_id, is_current);
CREATE INDEX IF NOT EXISTS ix_silver_rejected_records_run ON silver.rejected_records (pipeline_run_id);
