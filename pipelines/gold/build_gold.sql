-- Gold transform: silver.* -> gold.* per the fixed contract in
-- database/schemas/gold.sql. Reads Silver only (never raw Bronze), and
-- builds order_360 first so every other Gold table aggregates from it
-- instead of re-joining order_items/payment_events/refund_events again --
-- that is what keeps the five tables from multiplying facts against each
-- other.
--
-- Run by pipelines/run_pipeline.py through pipelines/ingestion/db.py's
-- apply_sql_template(), which substitutes {{PIPELINE_RUN_ID}}.
--
-- Metric definitions not pinned down by the README are documented in
-- data_contract_notes.md at the repo root (payment_status/return_status
-- vocabulary, campaign attribution window, "resolved order").

SET TIME ZONE 'UTC';

TRUNCATE TABLE
    gold.order_360,
    gold.customer_daily,
    gold.product_daily,
    gold.channel_campaign_daily,
    gold.executive_kpis_daily;

-- ---------------------------------------------------------------------
-- gold.order_360 -- one row per resolved order
-- ---------------------------------------------------------------------
WITH item_totals AS (
    SELECT
        oi.order_id,
        count(*) AS item_count,
        sum(oi.quantity) AS unit_quantity,
        sum(oi.line_amount) AS gross_merchandise_value,
        sum(oi.item_discount_amount) AS item_discount_amount
    FROM silver.order_items oi
    GROUP BY oi.order_id
),
promo_totals AS (
    SELECT order_id, count(DISTINCT promotion_id) AS promotion_count, sum(discount_amount) AS discount_amount
    FROM silver.order_promotions
    GROUP BY order_id
),
payment_totals AS (
    SELECT
        order_id,
        sum(amount) FILTER (WHERE status = 'captured') AS captured_payment_amount,
        bool_or(status = 'captured') AS has_captured,
        bool_or(status = 'authorized') AS has_pending
    FROM silver.payment_events
    GROUP BY order_id
),
refund_totals AS (
    SELECT order_id, sum(amount) FILTER (WHERE status = 'completed') AS refunded_amount
    FROM silver.refund_events
    GROUP BY order_id
),
return_totals AS (
    SELECT
        order_id,
        bool_or(status = 'completed') AS has_completed_return,
        bool_or(status = 'received') AS has_received_return,
        bool_or(status = 'requested') AS has_requested_return
    FROM silver.return_events
    GROUP BY order_id
),
first_orders AS (
    SELECT customer_id, min(order_ts) AS first_order_ts
    FROM silver.orders
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
INSERT INTO gold.order_360 (
    order_id, customer_id, order_date, sales_channel, store_id,
    gross_merchandise_value, discount_amount, shipping_revenue,
    captured_payment_amount, refunded_amount, net_revenue,
    item_count, unit_quantity, order_status, payment_status, return_status,
    promotion_count, first_order_flag, pipeline_run_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.sales_channel,
    o.store_id,
    COALESCE(it.gross_merchandise_value, 0),
    COALESCE(pr.discount_amount, 0) + COALESCE(it.item_discount_amount, 0),
    o.shipping_revenue,
    COALESCE(pt.captured_payment_amount, 0),
    COALESCE(rt.refunded_amount, 0),
    COALESCE(it.gross_merchandise_value, 0)
        - (COALESCE(pr.discount_amount, 0) + COALESCE(it.item_discount_amount, 0))
        + o.shipping_revenue - COALESCE(rt.refunded_amount, 0) AS net_revenue,
    COALESCE(it.item_count, 0),
    COALESCE(it.unit_quantity, 0),
    o.order_status,
    CASE
        WHEN pt.has_captured THEN 'captured'
        WHEN pt.has_pending THEN 'pending'
        WHEN pt.order_id IS NOT NULL THEN 'failed'
        ELSE 'no_payment'
    END,
    CASE
        WHEN ret.has_completed_return THEN 'returned'
        WHEN ret.has_received_return THEN 'received'
        WHEN ret.has_requested_return THEN 'return_requested'
        ELSE 'none'
    END,
    COALESCE(pr.promotion_count, 0),
    (fo.first_order_ts = o.order_ts),
    '{{PIPELINE_RUN_ID}}'
FROM silver.orders o
LEFT JOIN item_totals it ON it.order_id = o.order_id
LEFT JOIN promo_totals pr ON pr.order_id = o.order_id
LEFT JOIN payment_totals pt ON pt.order_id = o.order_id
LEFT JOIN refund_totals rt ON rt.order_id = o.order_id
LEFT JOIN return_totals ret ON ret.order_id = o.order_id
LEFT JOIN first_orders fo ON fo.customer_id = o.customer_id
WHERE o.has_valid_customer;

-- ---------------------------------------------------------------------
-- gold.customer_daily -- one row per customer per business date
-- ---------------------------------------------------------------------
WITH activity_dates AS (
    SELECT customer_id, order_date AS metric_date FROM gold.order_360
    UNION
    SELECT customer_id, (event_ts AT TIME ZONE 'UTC')::date AS metric_date
    FROM silver.support_events
    WHERE customer_id IS NOT NULL
),
order_agg AS (
    SELECT
        customer_id, order_date AS metric_date,
        count(*) AS order_count,
        sum(unit_quantity) AS unit_quantity,
        sum(gross_merchandise_value) AS gross_revenue,
        sum(net_revenue) AS net_revenue,
        sum(refunded_amount) AS refund_amount,
        count(*) FILTER (WHERE return_status <> 'none') AS return_count
    FROM gold.order_360
    GROUP BY customer_id, order_date
),
channel_agg AS (
    SELECT DISTINCT ON (customer_id, order_date)
        customer_id, order_date AS metric_date, sales_channel AS active_channel
    FROM gold.order_360
    GROUP BY customer_id, order_date, sales_channel
    ORDER BY customer_id, order_date, count(*) DESC, sales_channel
),
support_agg AS (
    SELECT customer_id, (event_ts AT TIME ZONE 'UTC')::date AS metric_date, count(*) AS support_contacts
    FROM silver.support_events
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, (event_ts AT TIME ZONE 'UTC')::date
),
first_orders AS (
    SELECT customer_id, min(order_date) AS first_order_date
    FROM gold.order_360
    GROUP BY customer_id
),
prior_orders AS (
    SELECT DISTINCT customer_id, order_date AS metric_date
    FROM gold.order_360
)
INSERT INTO gold.customer_daily (
    customer_id, metric_date, order_count, unit_quantity, gross_revenue, net_revenue,
    refund_amount, return_count, support_contacts, active_channel,
    new_customer_flag, repeat_customer_flag, pipeline_run_id
)
SELECT
    a.customer_id,
    a.metric_date,
    COALESCE(oa.order_count, 0),
    COALESCE(oa.unit_quantity, 0),
    COALESCE(oa.gross_revenue, 0),
    COALESCE(oa.net_revenue, 0),
    COALESCE(oa.refund_amount, 0),
    COALESCE(oa.return_count, 0),
    COALESCE(sa.support_contacts, 0),
    ca.active_channel,
    (fo.first_order_date = a.metric_date),
    EXISTS (
        SELECT 1 FROM prior_orders po
        WHERE po.customer_id = a.customer_id AND po.metric_date < a.metric_date
    ),
    '{{PIPELINE_RUN_ID}}'
FROM activity_dates a
LEFT JOIN order_agg oa ON oa.customer_id = a.customer_id AND oa.metric_date = a.metric_date
LEFT JOIN channel_agg ca ON ca.customer_id = a.customer_id AND ca.metric_date = a.metric_date
LEFT JOIN support_agg sa ON sa.customer_id = a.customer_id AND sa.metric_date = a.metric_date
LEFT JOIN first_orders fo ON fo.customer_id = a.customer_id;

-- ---------------------------------------------------------------------
-- gold.product_daily -- one row per product per business date
-- ---------------------------------------------------------------------
WITH order_lines AS (
    SELECT
        oi.product_id,
        o.order_date,
        oi.order_id,
        oi.quantity,
        oi.line_amount,
        oi.line_amount / NULLIF(o.gross_merchandise_value, 0) AS line_share,
        o.discount_amount,
        o.refunded_amount
    FROM silver.order_items oi
    JOIN gold.order_360 o ON o.order_id = oi.order_id
    WHERE oi.product_id IS NOT NULL
),
sales_agg AS (
    SELECT
        product_id,
        order_date AS metric_date,
        count(DISTINCT order_id) AS order_count,
        sum(quantity) AS units_sold,
        sum(line_amount) AS gross_revenue,
        sum(line_amount - COALESCE(line_share, 0) * (discount_amount + refunded_amount)) AS net_revenue
    FROM order_lines
    GROUP BY product_id, order_date
),
promo_agg AS (
    SELECT ol.product_id, ol.order_date AS metric_date, count(DISTINCT op.promotion_id) AS promotion_count
    FROM order_lines ol
    JOIN silver.order_promotions op ON op.order_id = ol.order_id
    GROUP BY ol.product_id, ol.order_date
),
-- The source has no line-item detail for returns (return_events only
-- carries order_id), so a completed return is treated as returning every
-- item on that order -- an order-level approximation, not a per-line fact.
return_agg AS (
    SELECT oi.product_id, o.order_date AS metric_date, sum(oi.quantity) AS refunded_units
    FROM silver.return_events re
    JOIN silver.order_items oi ON oi.order_id = re.order_id
    JOIN gold.order_360 o ON o.order_id = re.order_id
    WHERE re.status = 'completed'
    GROUP BY oi.product_id, o.order_date
),
inventory_agg AS (
    SELECT product_id, snapshot_date AS metric_date,
           sum(available_quantity) AS available_inventory,
           sum(reserved_quantity) AS reserved_inventory
    FROM silver.inventory_snapshots
    GROUP BY product_id, snapshot_date
),
activity_dates AS (
    SELECT product_id, metric_date FROM sales_agg
    UNION
    SELECT product_id, metric_date FROM inventory_agg
)
INSERT INTO gold.product_daily (
    product_id, metric_date, active_category, units_sold, order_count,
    gross_revenue, net_revenue, refunded_units, available_inventory,
    reserved_inventory, stockout_flag, promotion_count, pipeline_run_id
)
SELECT
    a.product_id,
    a.metric_date,
    (
        SELECT pc.category_name FROM silver.product_categories pc
        WHERE pc.product_id = a.product_id
          AND pc.valid_from <= a.metric_date
          AND (pc.valid_to IS NULL OR pc.valid_to > a.metric_date)
        ORDER BY pc.valid_from DESC
        LIMIT 1
    ),
    COALESCE(sa.units_sold, 0),
    COALESCE(sa.order_count, 0),
    COALESCE(sa.gross_revenue, 0),
    COALESCE(sa.net_revenue, 0),
    COALESCE(ra.refunded_units, 0),
    ia.available_inventory,
    ia.reserved_inventory,
    CASE WHEN ia.available_inventory IS NULL THEN NULL ELSE ia.available_inventory <= 0 END,
    COALESCE(pa.promotion_count, 0),
    '{{PIPELINE_RUN_ID}}'
FROM activity_dates a
LEFT JOIN sales_agg sa ON sa.product_id = a.product_id AND sa.metric_date = a.metric_date
LEFT JOIN promo_agg pa ON pa.product_id = a.product_id AND pa.metric_date = a.metric_date
LEFT JOIN return_agg ra ON ra.product_id = a.product_id AND ra.metric_date = a.metric_date
LEFT JOIN inventory_agg ia ON ia.product_id = a.product_id AND ia.metric_date = a.metric_date;

-- ---------------------------------------------------------------------
-- gold.channel_campaign_daily -- one row per date, channel, campaign
--
-- Attribution: last-touch. An order is attributed to the most recent
-- web_event for the same customer and channel, with a non-null
-- campaign_id, in the 7 days before the order.
-- ---------------------------------------------------------------------
WITH touches AS (
    SELECT customer_id, sales_channel, campaign_id, event_ts
    FROM silver.web_events
    WHERE campaign_id IS NOT NULL AND customer_id IS NOT NULL
),
attributed_orders AS (
    SELECT
        o.order_id, o.order_date, o.sales_channel, o.gross_merchandise_value,
        o.net_revenue, o.refunded_amount, o.customer_id,
        (
            SELECT t.campaign_id FROM touches t
            WHERE t.customer_id = o.customer_id
              AND t.sales_channel = o.sales_channel
              AND t.event_ts <= so.order_ts
              AND t.event_ts >= so.order_ts - INTERVAL '7 days'
            ORDER BY t.event_ts DESC
            LIMIT 1
        ) AS campaign_id
    FROM gold.order_360 o
    JOIN silver.orders so ON so.order_id = o.order_id
),
order_agg AS (
    SELECT
        order_date AS metric_date, sales_channel, campaign_id,
        count(*) AS attributed_orders,
        count(DISTINCT customer_id) AS attributed_customers,
        sum(gross_merchandise_value) AS gross_revenue,
        sum(net_revenue) AS net_revenue,
        sum(refunded_amount) AS refunds
    FROM attributed_orders
    WHERE campaign_id IS NOT NULL
    GROUP BY order_date, sales_channel, campaign_id
),
touch_agg AS (
    SELECT
        (event_ts AT TIME ZONE 'UTC')::date AS metric_date, sales_channel, campaign_id,
        count(DISTINCT customer_id) AS touched_customers
    FROM touches
    GROUP BY (event_ts AT TIME ZONE 'UTC')::date, sales_channel, campaign_id
),
spend_agg AS (
    SELECT spend_date AS metric_date, sales_channel, campaign_id, spend_amount AS campaign_spend
    FROM silver.campaign_spend
),
keys AS (
    SELECT metric_date, sales_channel, campaign_id FROM order_agg
    UNION
    SELECT metric_date, sales_channel, campaign_id FROM spend_agg
)
INSERT INTO gold.channel_campaign_daily (
    metric_date, sales_channel, campaign_id, campaign_spend, attributed_orders,
    attributed_customers, gross_revenue, net_revenue, refunds, roas, conversion_rate,
    pipeline_run_id
)
SELECT
    k.metric_date,
    k.sales_channel,
    k.campaign_id,
    COALESCE(sp.campaign_spend, 0),
    COALESCE(oa.attributed_orders, 0),
    COALESCE(oa.attributed_customers, 0),
    COALESCE(oa.gross_revenue, 0),
    COALESCE(oa.net_revenue, 0),
    COALESCE(oa.refunds, 0),
    CASE WHEN COALESCE(sp.campaign_spend, 0) > 0 THEN COALESCE(oa.net_revenue, 0) / sp.campaign_spend ELSE NULL END,
    CASE WHEN COALESCE(ta.touched_customers, 0) > 0
         THEN COALESCE(oa.attributed_orders, 0)::numeric / ta.touched_customers
         ELSE NULL END,
    '{{PIPELINE_RUN_ID}}'
FROM keys k
LEFT JOIN order_agg oa ON oa.metric_date = k.metric_date AND oa.sales_channel = k.sales_channel AND oa.campaign_id = k.campaign_id
LEFT JOIN spend_agg sp ON sp.metric_date = k.metric_date AND sp.sales_channel = k.sales_channel AND sp.campaign_id = k.campaign_id
LEFT JOIN touch_agg ta ON ta.metric_date = k.metric_date AND ta.sales_channel = k.sales_channel AND ta.campaign_id = k.campaign_id;

-- ---------------------------------------------------------------------
-- gold.executive_kpis_daily -- one row per business date
--
-- "resolved order" (for return_rate) = an order that actually reached a
-- final, fulfilled outcome: FULFILLED or RETURNED. PLACED/CONFIRMED are
-- still in flight and CANCELLED never completed, so neither belongs in
-- the denominator of "how often does a completed order come back".
-- Source status values are upper-case (see data_contract_notes.md).
-- ---------------------------------------------------------------------
WITH order_agg AS (
    SELECT
        order_date AS metric_date,
        count(*) AS total_orders,
        sum(gross_merchandise_value) AS gross_revenue,
        sum(net_revenue) AS net_revenue,
        count(*) FILTER (WHERE payment_status = 'captured') AS captured_orders,
        count(*) FILTER (WHERE payment_status = 'captured' AND refunded_amount > 0) AS refunded_orders,
        count(*) FILTER (WHERE order_status IN ('FULFILLED', 'RETURNED')) AS resolved_orders,
        count(*) FILTER (WHERE order_status IN ('FULFILLED', 'RETURNED') AND return_status <> 'none') AS returned_orders
    FROM gold.order_360
    GROUP BY order_date
),
customer_agg AS (
    SELECT
        metric_date,
        count(*) AS active_customers,
        count(*) FILTER (WHERE repeat_customer_flag) AS repeat_customers,
        count(*) FILTER (WHERE support_contacts > 0) AS customers_with_support
    FROM gold.customer_daily
    GROUP BY metric_date
),
product_agg AS (
    SELECT
        metric_date,
        count(*) FILTER (WHERE stockout_flag IS NOT NULL) AS tracked_products,
        count(*) FILTER (WHERE stockout_flag) AS stocked_out_products
    FROM gold.product_daily
    GROUP BY metric_date
)
INSERT INTO gold.executive_kpis_daily (
    metric_date, total_orders, gross_revenue, net_revenue, average_order_value,
    refund_rate, return_rate, repeat_customer_rate, stockout_rate, active_customers,
    support_contact_rate, data_freshness_utc, pipeline_run_id
)
SELECT
    oa.metric_date,
    oa.total_orders,
    oa.gross_revenue,
    oa.net_revenue,
    CASE WHEN oa.total_orders > 0 THEN oa.net_revenue / oa.total_orders ELSE 0 END,
    CASE WHEN oa.captured_orders > 0 THEN oa.refunded_orders::numeric / oa.captured_orders ELSE 0 END,
    CASE WHEN oa.resolved_orders > 0 THEN oa.returned_orders::numeric / oa.resolved_orders ELSE 0 END,
    CASE WHEN COALESCE(ca.active_customers, 0) > 0 THEN ca.repeat_customers::numeric / ca.active_customers ELSE 0 END,
    CASE WHEN COALESCE(pa.tracked_products, 0) > 0 THEN pa.stocked_out_products::numeric / pa.tracked_products ELSE 0 END,
    COALESCE(ca.active_customers, 0),
    CASE WHEN COALESCE(ca.active_customers, 0) > 0 THEN ca.customers_with_support::numeric / ca.active_customers ELSE 0 END,
    now(),
    '{{PIPELINE_RUN_ID}}'
FROM order_agg oa
LEFT JOIN customer_agg ca ON ca.metric_date = oa.metric_date
LEFT JOIN product_agg pa ON pa.metric_date = oa.metric_date;
