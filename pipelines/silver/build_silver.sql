-- Silver transform: bronze.* (raw JSONB evidence) -> silver.* (typed,
-- deduplicated, validated, lineage-tracked entities).
--
-- Run by pipelines/run_pipeline.py through pipelines/ingestion/db.py's
-- apply_sql_template(), which substitutes {{PIPELINE_RUN_ID}} below with
-- the current Airflow run id before this script reaches PostgreSQL.
--
-- Strategy: full refresh. Every run truncates Silver and rebuilds it from
-- the *entire* accumulated Bronze history (not just the newest ingestion
-- run), so a rerun is idempotent and late-arriving Bronze rows from an
-- earlier file drop are automatically reconciled on the next Silver build.
--
-- Dedup key: for dimension/event files, the natural id extracted into
-- bronze.*.source_record_id during Bronze load (event_id for the five
-- event families); ties are broken by (ingested_at DESC, bronze_id DESC),
-- i.e. the newest evidence wins. This step alone is *not* enough for the
-- four lifecycle event families (payment/refund/return/support) -- see
-- the "state reconstruction" section below.
--
-- Field names match the real Google Drive export -- see
-- data_contract_notes.md for the confirmed shape of every source file.

SET TIME ZONE 'UTC';

TRUNCATE TABLE
    silver.customers,
    silver.customer_profiles,
    silver.customer_addresses,
    silver.products,
    silver.product_categories,
    silver.stores,
    silver.sales_channels,
    silver.promotions,
    silver.orders,
    silver.order_items,
    silver.order_promotions,
    silver.payment_events,
    silver.refund_events,
    silver.return_events,
    silver.support_events,
    silver.web_events,
    silver.inventory_snapshots,
    silver.campaign_spend,
    silver.city_reference,
    silver.rejected_records;

-- ---------------------------------------------------------------------
-- customers
-- ---------------------------------------------------------------------
WITH dups AS (
    SELECT source_record_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM bronze.customers
    WHERE source_record_id IS NOT NULL
    GROUP BY source_record_id
),
ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.customers b
    WHERE b.source_record_id IS NOT NULL
)
INSERT INTO silver.customers (
    customer_id, city_id, customer_segment, created_at,
    source_bronze_id, duplicate_bronze_ids, pipeline_run_id
)
SELECT
    r.source_record_id,
    r.raw_payload->>'city_id',
    r.raw_payload->>'customer_segment',
    (r.raw_payload->>'created_at_utc')::timestamptz,
    r.bronze_id,
    array_remove(d.bronze_ids, r.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM ranked r
JOIN dups d ON d.source_record_id = r.source_record_id
WHERE r.rn = 1;

INSERT INTO silver.rejected_records (source_table, bronze_id, natural_key, rejection_reason, raw_payload, pipeline_run_id)
SELECT 'bronze.customers', bronze_id, source_record_id, 'missing customer_id', raw_payload, '{{PIPELINE_RUN_ID}}'
FROM bronze.customers
WHERE source_record_id IS NULL;

-- ---------------------------------------------------------------------
-- customer_profiles (SCD2 on customer_id) -- the source already ships
-- valid_from_utc/valid_to_utc per version, so this only dedups repeated
-- Bronze ingestions of the same version and flags the newest as current.
-- ---------------------------------------------------------------------
WITH deduped AS (
    SELECT
        b.source_record_id AS customer_id,
        b.bronze_id,
        (b.raw_payload->>'valid_from_utc')::timestamptz AS valid_from,
        (b.raw_payload->>'valid_to_utc')::timestamptz AS valid_to,
        b.raw_payload->>'city_id' AS city_id,
        b.raw_payload->>'customer_segment' AS customer_segment,
        row_number() OVER (
            PARTITION BY b.source_record_id, b.raw_payload->>'valid_from_utc'
            ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.customer_profiles b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->>'valid_from_utc' IS NOT NULL
),
versions AS (
    SELECT
        customer_id, bronze_id, valid_from, valid_to, city_id, customer_segment,
        row_number() OVER (PARTITION BY customer_id ORDER BY valid_from DESC) AS recency_rank
    FROM deduped
    WHERE rn = 1
)
INSERT INTO silver.customer_profiles (
    customer_id, city_id, customer_segment, valid_from, valid_to, is_current, source_bronze_id, pipeline_run_id
)
SELECT customer_id, city_id, customer_segment, valid_from, valid_to, (recency_rank = 1), bronze_id, '{{PIPELINE_RUN_ID}}'
FROM versions;

INSERT INTO silver.rejected_records (source_table, bronze_id, natural_key, rejection_reason, raw_payload, pipeline_run_id)
SELECT 'bronze.customer_profiles', bronze_id, source_record_id, 'missing customer_id or valid_from_utc', raw_payload, '{{PIPELINE_RUN_ID}}'
FROM bronze.customer_profiles
WHERE source_record_id IS NULL OR raw_payload->>'valid_from_utc' IS NULL;

-- ---------------------------------------------------------------------
-- customer_addresses (SCD2 on customer_id, same pass-through pattern)
-- ---------------------------------------------------------------------
WITH deduped AS (
    SELECT
        b.source_record_id AS address_id,
        b.bronze_id,
        b.raw_payload->>'customer_id' AS customer_id,
        b.raw_payload->>'city_id' AS city_id,
        (b.raw_payload->>'valid_from_utc')::timestamptz AS valid_from,
        (b.raw_payload->>'valid_to_utc')::timestamptz AS valid_to,
        row_number() OVER (
            PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.customer_addresses b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->>'customer_id' IS NOT NULL
),
versions AS (
    SELECT
        address_id, bronze_id, customer_id, city_id, valid_from, valid_to,
        row_number() OVER (PARTITION BY customer_id ORDER BY valid_from DESC) AS recency_rank
    FROM deduped
    WHERE rn = 1
)
INSERT INTO silver.customer_addresses (
    address_id, customer_id, city_id, valid_from, valid_to, is_current, source_bronze_id, pipeline_run_id
)
SELECT address_id, customer_id, city_id, valid_from, valid_to, (recency_rank = 1), bronze_id, '{{PIPELINE_RUN_ID}}'
FROM versions;

-- ---------------------------------------------------------------------
-- products
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.products b
    WHERE b.source_record_id IS NOT NULL
)
INSERT INTO silver.products (product_id, product_name, sku, unit_price, source_bronze_id, pipeline_run_id)
SELECT
    r.source_record_id,
    r.raw_payload->>'product_name',
    r.raw_payload->>'sku',
    (r.raw_payload->>'unit_price')::numeric,
    r.bronze_id,
    '{{PIPELINE_RUN_ID}}'
FROM ranked r
WHERE r.rn = 1;

INSERT INTO silver.rejected_records (source_table, bronze_id, natural_key, rejection_reason, raw_payload, pipeline_run_id)
SELECT 'bronze.products', bronze_id, source_record_id, 'missing product_id', raw_payload, '{{PIPELINE_RUN_ID}}'
FROM bronze.products
WHERE source_record_id IS NULL;

-- ---------------------------------------------------------------------
-- product_categories (SCD2 on product_id, pass-through valid_from/to)
-- ---------------------------------------------------------------------
WITH deduped AS (
    SELECT
        b.source_record_id AS product_id,
        b.bronze_id,
        b.raw_payload->>'category_id' AS category_id,
        b.raw_payload->>'category_name' AS category_name,
        (b.raw_payload->>'valid_from_utc')::timestamptz AS valid_from,
        (b.raw_payload->>'valid_to_utc')::timestamptz AS valid_to,
        row_number() OVER (
            PARTITION BY b.source_record_id, b.raw_payload->>'valid_from_utc'
            ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.product_categories b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->>'valid_from_utc' IS NOT NULL
),
versions AS (
    SELECT
        product_id, bronze_id, category_id, category_name, valid_from, valid_to,
        row_number() OVER (PARTITION BY product_id ORDER BY valid_from DESC) AS recency_rank
    FROM deduped
    WHERE rn = 1 AND category_name IS NOT NULL
)
INSERT INTO silver.product_categories (
    product_id, category_id, category_name, valid_from, valid_to, is_current, source_bronze_id, pipeline_run_id
)
SELECT product_id, category_id, category_name, valid_from, valid_to, (recency_rank = 1), bronze_id, '{{PIPELINE_RUN_ID}}'
FROM versions;

-- ---------------------------------------------------------------------
-- stores
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.stores b
    WHERE b.source_record_id IS NOT NULL
)
INSERT INTO silver.stores (store_id, store_name, city_id, location_type, source_bronze_id, pipeline_run_id)
SELECT
    r.source_record_id,
    r.raw_payload->>'store_name',
    r.raw_payload->>'city_id',
    r.raw_payload->>'location_type',
    r.bronze_id,
    '{{PIPELINE_RUN_ID}}'
FROM ranked r
WHERE r.rn = 1;

-- ---------------------------------------------------------------------
-- sales_channels
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.sales_channels b
    WHERE b.source_record_id IS NOT NULL
)
INSERT INTO silver.sales_channels (channel_id, channel_name, source_bronze_id, pipeline_run_id)
SELECT
    r.source_record_id,
    COALESCE(r.raw_payload->>'channel_name', r.source_record_id),
    r.bronze_id,
    '{{PIPELINE_RUN_ID}}'
FROM ranked r
WHERE r.rn = 1;

-- ---------------------------------------------------------------------
-- promotions
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.promotions b
    WHERE b.source_record_id IS NOT NULL
)
INSERT INTO silver.promotions (
    promotion_id, promotion_code, promotion_type, discount_rate, starts_at, ends_at,
    source_bronze_id, pipeline_run_id
)
SELECT
    r.source_record_id,
    r.raw_payload->>'promotion_code',
    r.raw_payload->>'promotion_type',
    (r.raw_payload->>'discount_rate')::numeric,
    (r.raw_payload->>'start_date')::date,
    (r.raw_payload->>'end_date')::date,
    r.bronze_id,
    '{{PIPELINE_RUN_ID}}'
FROM ranked r
WHERE r.rn = 1;

-- ---------------------------------------------------------------------
-- orders
-- ---------------------------------------------------------------------
WITH dups AS (
    SELECT source_record_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM bronze.orders
    WHERE source_record_id IS NOT NULL
    GROUP BY source_record_id
),
ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.orders b
    WHERE b.source_record_id IS NOT NULL
      AND b.raw_payload->>'ordered_at_utc' IS NOT NULL
),
resolved AS (
    SELECT
        r.source_record_id AS order_id,
        r.raw_payload->>'customer_id' AS customer_id,
        r.raw_payload->>'store_id' AS store_id,
        COALESCE(r.raw_payload->>'sales_channel', 'UNKNOWN') AS sales_channel,
        (r.raw_payload->>'ordered_at_utc')::timestamptz AS order_ts,
        COALESCE(r.raw_payload->>'status', 'UNKNOWN') AS order_status,
        COALESCE((r.raw_payload->>'shipping_revenue')::numeric, 0) AS shipping_revenue,
        r.bronze_id,
        d.bronze_ids
    FROM ranked r
    JOIN dups d ON d.source_record_id = r.source_record_id
    WHERE r.rn = 1
)
INSERT INTO silver.orders (
    order_id, customer_id, store_id, sales_channel, order_ts, order_date,
    order_status, shipping_revenue, has_valid_customer, source_bronze_id, duplicate_bronze_ids, pipeline_run_id
)
SELECT
    resolved.order_id,
    resolved.customer_id,
    resolved.store_id,
    resolved.sales_channel,
    resolved.order_ts,
    (resolved.order_ts AT TIME ZONE 'UTC')::date,
    resolved.order_status,
    resolved.shipping_revenue,
    resolved.customer_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM silver.customers c WHERE c.customer_id = resolved.customer_id
    ),
    resolved.bronze_id,
    array_remove(resolved.bronze_ids, resolved.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM resolved;

INSERT INTO silver.rejected_records (source_table, bronze_id, natural_key, rejection_reason, raw_payload, pipeline_run_id)
SELECT 'bronze.orders', bronze_id, source_record_id, 'missing order_id or ordered_at_utc', raw_payload, '{{PIPELINE_RUN_ID}}'
FROM bronze.orders
WHERE source_record_id IS NULL OR raw_payload->>'ordered_at_utc' IS NULL;

-- ---------------------------------------------------------------------
-- order_items (reject negative quantity per README requirement)
-- ---------------------------------------------------------------------
WITH dups AS (
    SELECT source_record_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM bronze.order_items
    WHERE source_record_id IS NOT NULL
    GROUP BY source_record_id
),
ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.order_items b
    WHERE b.source_record_id IS NOT NULL
),
resolved AS (
    SELECT
        r.source_record_id AS order_item_id,
        r.raw_payload->>'order_id' AS order_id,
        r.raw_payload->>'product_id' AS product_id,
        (r.raw_payload->>'quantity')::integer AS quantity,
        (r.raw_payload->>'unit_price')::numeric AS unit_price,
        COALESCE((r.raw_payload->>'item_discount_amount')::numeric, 0) AS item_discount_amount,
        r.bronze_id,
        d.bronze_ids
    FROM ranked r
    JOIN dups d ON d.source_record_id = r.source_record_id
    WHERE r.rn = 1
)
INSERT INTO silver.order_items (
    order_item_id, order_id, product_id, quantity, unit_price, item_discount_amount, line_amount,
    has_valid_product, source_bronze_id, duplicate_bronze_ids, pipeline_run_id
)
SELECT
    resolved.order_item_id,
    resolved.order_id,
    resolved.product_id,
    resolved.quantity,
    resolved.unit_price,
    resolved.item_discount_amount,
    resolved.quantity * resolved.unit_price,
    resolved.product_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM silver.products p WHERE p.product_id = resolved.product_id
    ),
    resolved.bronze_id,
    array_remove(resolved.bronze_ids, resolved.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM resolved
WHERE resolved.quantity IS NOT NULL AND resolved.quantity >= 0;

INSERT INTO silver.rejected_records (source_table, bronze_id, natural_key, rejection_reason, raw_payload, pipeline_run_id)
SELECT 'bronze.order_items', bronze_id, source_record_id, 'missing order_item_id', raw_payload, '{{PIPELINE_RUN_ID}}'
FROM bronze.order_items
WHERE source_record_id IS NULL;

INSERT INTO silver.rejected_records (source_table, bronze_id, natural_key, rejection_reason, raw_payload, pipeline_run_id)
SELECT 'bronze.order_items', bronze_id, source_record_id, 'negative or missing quantity', raw_payload, '{{PIPELINE_RUN_ID}}'
FROM bronze.order_items
WHERE source_record_id IS NOT NULL
  AND ((raw_payload->>'quantity')::integer IS NULL OR (raw_payload->>'quantity')::integer < 0)
  AND bronze_id IN (
      SELECT bronze_id FROM (
          SELECT bronze_id, row_number() OVER (
              PARTITION BY source_record_id ORDER BY ingested_at DESC, bronze_id DESC
          ) AS rn
          FROM bronze.order_items WHERE source_record_id IS NOT NULL
      ) latest WHERE rn = 1
  );

-- ---------------------------------------------------------------------
-- order_promotions
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT
        b.*,
        row_number() OVER (
            PARTITION BY b.raw_payload->>'order_id', b.raw_payload->>'promotion_id'
            ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.order_promotions b
    WHERE b.raw_payload->>'order_id' IS NOT NULL AND b.raw_payload->>'promotion_id' IS NOT NULL
)
INSERT INTO silver.order_promotions (order_id, promotion_id, discount_amount, source_bronze_id, pipeline_run_id)
SELECT
    r.raw_payload->>'order_id',
    r.raw_payload->>'promotion_id',
    COALESCE((r.raw_payload->>'discount_amount')::numeric, 0),
    r.bronze_id,
    '{{PIPELINE_RUN_ID}}'
FROM ranked r
WHERE r.rn = 1;

-- ---------------------------------------------------------------------
-- Lifecycle events (payment/refund/return/support): raw rows are
-- {event_id, event_type, occurred_at_utc, payload}. Step 1 dedups
-- repeated Bronze ingestion of the same event_id. Step 2 collapses every
-- event_id belonging to the same underlying entity (payment_id/
-- refund_id/return_id/ticket_id) down to its terminal status.
-- ---------------------------------------------------------------------

-- payment_events -> one row per payment_id
WITH deduped_events AS (
    SELECT
        b.bronze_id,
        b.raw_payload->'payload'->>'payment_id' AS payment_id,
        b.raw_payload->'payload'->>'order_id' AS order_id,
        (b.raw_payload->'payload'->>'amount')::numeric AS amount,
        b.raw_payload->>'event_type' AS event_type,
        (b.raw_payload->>'occurred_at_utc')::timestamptz AS occurred_at,
        row_number() OVER (
            PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.payment_events b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->'payload'->>'payment_id' IS NOT NULL
),
events AS (
    SELECT bronze_id, payment_id, order_id, amount, event_type, occurred_at
    FROM deduped_events WHERE rn = 1
),
totals AS (
    SELECT payment_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM events GROUP BY payment_id
),
ranked_terminal AS (
    SELECT *, row_number() OVER (
        PARTITION BY payment_id
        ORDER BY CASE event_type
            WHEN 'PAYMENT_CAPTURED' THEN 3
            WHEN 'PAYMENT_FAILED' THEN 2
            ELSE 1
        END DESC, occurred_at DESC
    ) AS rn
    FROM events
)
INSERT INTO silver.payment_events (payment_id, order_id, event_ts, amount, status, source_bronze_id, duplicate_bronze_ids, pipeline_run_id)
SELECT
    t.payment_id, t.order_id, t.occurred_at, t.amount,
    CASE t.event_type WHEN 'PAYMENT_CAPTURED' THEN 'captured' WHEN 'PAYMENT_FAILED' THEN 'failed' ELSE 'authorized' END,
    t.bronze_id,
    array_remove(tot.bronze_ids, t.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM ranked_terminal t
JOIN totals tot ON tot.payment_id = t.payment_id
WHERE t.rn = 1;

-- refund_events -> one row per refund_id
WITH deduped_events AS (
    SELECT
        b.bronze_id,
        b.raw_payload->'payload'->>'refund_id' AS refund_id,
        b.raw_payload->'payload'->>'order_id' AS order_id,
        (b.raw_payload->'payload'->>'amount')::numeric AS amount,
        b.raw_payload->>'event_type' AS event_type,
        (b.raw_payload->>'occurred_at_utc')::timestamptz AS occurred_at,
        row_number() OVER (
            PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.refund_events b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->'payload'->>'refund_id' IS NOT NULL
),
events AS (
    SELECT bronze_id, refund_id, order_id, amount, event_type, occurred_at
    FROM deduped_events WHERE rn = 1
),
totals AS (
    SELECT refund_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM events GROUP BY refund_id
),
ranked_terminal AS (
    SELECT *, row_number() OVER (
        PARTITION BY refund_id
        ORDER BY CASE event_type WHEN 'REFUND_COMPLETED' THEN 2 ELSE 1 END DESC, occurred_at DESC
    ) AS rn
    FROM events
)
INSERT INTO silver.refund_events (refund_id, order_id, event_ts, amount, status, source_bronze_id, duplicate_bronze_ids, pipeline_run_id)
SELECT
    t.refund_id, t.order_id, t.occurred_at, t.amount,
    CASE t.event_type WHEN 'REFUND_COMPLETED' THEN 'completed' ELSE 'issued' END,
    t.bronze_id,
    array_remove(tot.bronze_ids, t.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM ranked_terminal t
JOIN totals tot ON tot.refund_id = t.refund_id
WHERE t.rn = 1;

-- return_events -> one row per return_id (no line-item/quantity detail in the source)
WITH deduped_events AS (
    SELECT
        b.bronze_id,
        b.raw_payload->'payload'->>'return_id' AS return_id,
        b.raw_payload->'payload'->>'order_id' AS order_id,
        b.raw_payload->>'event_type' AS event_type,
        (b.raw_payload->>'occurred_at_utc')::timestamptz AS occurred_at,
        row_number() OVER (
            PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.return_events b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->'payload'->>'return_id' IS NOT NULL
),
events AS (
    SELECT bronze_id, return_id, order_id, event_type, occurred_at
    FROM deduped_events WHERE rn = 1
),
totals AS (
    SELECT return_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM events GROUP BY return_id
),
ranked_terminal AS (
    SELECT *, row_number() OVER (
        PARTITION BY return_id
        ORDER BY CASE event_type
            WHEN 'RETURN_CLOSED' THEN 3
            WHEN 'RETURN_RECEIVED' THEN 2
            ELSE 1
        END DESC, occurred_at DESC
    ) AS rn
    FROM events
)
INSERT INTO silver.return_events (return_id, order_id, event_ts, status, source_bronze_id, duplicate_bronze_ids, pipeline_run_id)
SELECT
    t.return_id, t.order_id, t.occurred_at,
    CASE t.event_type
        WHEN 'RETURN_CLOSED' THEN 'completed'
        WHEN 'RETURN_RECEIVED' THEN 'received'
        ELSE 'requested'
    END,
    t.bronze_id,
    array_remove(tot.bronze_ids, t.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM ranked_terminal t
JOIN totals tot ON tot.return_id = t.return_id
WHERE t.rn = 1;

-- support_events -> one row per ticket_id ("reason" only appears on the
-- CREATED event, so it is carried separately rather than lost when the
-- terminal row is CLOSED).
WITH deduped_events AS (
    SELECT
        b.bronze_id,
        b.raw_payload->'payload'->>'ticket_id' AS ticket_id,
        b.raw_payload->'payload'->>'customer_id' AS customer_id,
        b.raw_payload->'payload'->>'order_id' AS order_id,
        b.raw_payload->'payload'->>'reason' AS reason,
        b.raw_payload->>'event_type' AS event_type,
        (b.raw_payload->>'occurred_at_utc')::timestamptz AS occurred_at,
        row_number() OVER (
            PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
        ) AS rn
    FROM bronze.support_events b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->'payload'->>'ticket_id' IS NOT NULL
),
events AS (
    SELECT bronze_id, ticket_id, customer_id, order_id, reason, event_type, occurred_at
    FROM deduped_events WHERE rn = 1
),
totals AS (
    SELECT
        ticket_id,
        array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids,
        max(reason) AS reason
    FROM events GROUP BY ticket_id
),
ranked_terminal AS (
    SELECT *, row_number() OVER (
        PARTITION BY ticket_id
        ORDER BY CASE event_type WHEN 'SUPPORT_TICKET_CLOSED' THEN 2 ELSE 1 END DESC, occurred_at DESC
    ) AS rn
    FROM events
)
INSERT INTO silver.support_events (ticket_id, customer_id, order_id, event_ts, reason, status, source_bronze_id, duplicate_bronze_ids, pipeline_run_id)
SELECT
    t.ticket_id, t.customer_id, t.order_id, t.occurred_at, tot.reason,
    CASE t.event_type WHEN 'SUPPORT_TICKET_CLOSED' THEN 'closed' ELSE 'open' END,
    t.bronze_id,
    array_remove(tot.bronze_ids, t.bronze_id),
    '{{PIPELINE_RUN_ID}}'
FROM ranked_terminal t
JOIN totals tot ON tot.ticket_id = t.ticket_id
WHERE t.rn = 1;

-- web_events -> flat log, one row per event (ADD_TO_CART/PAGE_VIEW/PURCHASE
-- are independent signals, not states of one object).
WITH dups AS (
    SELECT source_record_id, array_agg(bronze_id ORDER BY bronze_id) AS bronze_ids
    FROM bronze.web_events WHERE source_record_id IS NOT NULL GROUP BY source_record_id
),
ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.web_events b
    WHERE b.source_record_id IS NOT NULL AND b.raw_payload->>'occurred_at_utc' IS NOT NULL
)
INSERT INTO silver.web_events (
    web_event_id, customer_id, session_id, order_id, event_ts, event_type, sales_channel, campaign_id,
    source_bronze_id, duplicate_bronze_ids, pipeline_run_id
)
SELECT
    r.source_record_id,
    r.raw_payload->'payload'->>'customer_id',
    r.raw_payload->'payload'->>'session_id',
    r.raw_payload->'payload'->>'order_id',
    (r.raw_payload->>'occurred_at_utc')::timestamptz,
    r.raw_payload->>'event_type',
    r.raw_payload->'payload'->>'channel',
    r.raw_payload->'payload'->>'campaign_id',
    r.bronze_id, array_remove(d.bronze_ids, r.bronze_id), '{{PIPELINE_RUN_ID}}'
FROM ranked r JOIN dups d ON d.source_record_id = r.source_record_id
WHERE r.rn = 1;

-- ---------------------------------------------------------------------
-- inventory_snapshots (grain: product + store + day, last snapshot wins).
-- The CSV column is "location_id"; it shares the same id space as
-- stores.store_id (e.g. "STORE-01").
-- ---------------------------------------------------------------------
WITH parsed AS (
    SELECT
        b.bronze_id,
        b.raw_payload->>'product_id' AS product_id,
        b.raw_payload->>'location_id' AS store_id,
        (b.raw_payload->>'snapshot_date')::date AS snapshot_date,
        (b.raw_payload->>'available_quantity')::integer AS available_quantity,
        COALESCE((b.raw_payload->>'reserved_quantity')::integer, 0) AS reserved_quantity,
        b.ingested_at
    FROM bronze.inventory_snapshots b
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY product_id, store_id, snapshot_date ORDER BY ingested_at DESC, bronze_id DESC
    ) AS rn
    FROM parsed
    WHERE product_id IS NOT NULL AND store_id IS NOT NULL AND snapshot_date IS NOT NULL
)
INSERT INTO silver.inventory_snapshots (
    product_id, store_id, snapshot_date, available_quantity, reserved_quantity, source_bronze_id, pipeline_run_id
)
SELECT product_id, store_id, snapshot_date, available_quantity, reserved_quantity, bronze_id, '{{PIPELINE_RUN_ID}}'
FROM ranked
WHERE rn = 1;

-- ---------------------------------------------------------------------
-- campaign_spend (grain: campaign + channel + day). The CSV column is
-- "channel", sharing the same id space as sales_channels.channel_id.
-- ---------------------------------------------------------------------
WITH parsed AS (
    SELECT
        b.bronze_id,
        b.raw_payload->>'campaign_id' AS campaign_id,
        b.raw_payload->>'channel' AS sales_channel,
        (b.raw_payload->>'spend_date')::date AS spend_date,
        (b.raw_payload->>'spend_amount')::numeric AS spend_amount,
        b.ingested_at
    FROM bronze.campaign_spend b
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY campaign_id, sales_channel, spend_date ORDER BY ingested_at DESC, bronze_id DESC
    ) AS rn
    FROM parsed
    WHERE campaign_id IS NOT NULL AND sales_channel IS NOT NULL AND spend_date IS NOT NULL
)
INSERT INTO silver.campaign_spend (campaign_id, sales_channel, spend_date, spend_amount, source_bronze_id, pipeline_run_id)
SELECT campaign_id, sales_channel, spend_date, COALESCE(spend_amount, 0), bronze_id, '{{PIPELINE_RUN_ID}}'
FROM ranked
WHERE rn = 1;

-- ---------------------------------------------------------------------
-- city_reference
-- ---------------------------------------------------------------------
WITH ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.source_record_id ORDER BY b.ingested_at DESC, b.bronze_id DESC
    ) AS rn
    FROM bronze.city_reference b
    WHERE b.source_record_id IS NOT NULL
)
INSERT INTO silver.city_reference (city_id, city_name, country, source_bronze_id, pipeline_run_id)
SELECT r.source_record_id, r.raw_payload->>'city_name', r.raw_payload->>'country', r.bronze_id, '{{PIPELINE_RUN_ID}}'
FROM ranked r
WHERE r.rn = 1;
