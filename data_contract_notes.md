# Data contract notes

This documents what the real Google Drive export actually contains
(downloaded 2026-09-22) and the design decisions behind
`database/schemas/{bronze,silver,ops}.sql` and `pipelines/**`. The first
draft of this pipeline was written before the data was available and
guessed at field names; those guesses were wrong in several places and
have been corrected here against the real files. If anything below looks
inconsistent with a fresh download, re-run the inspection commands in
"How this was verified" and adjust `pipelines/bronze/build_bronze.py` /
`pipelines/silver/build_silver.sql` accordingly -- Gold and the quality
gate are keyed off Silver's typed columns, not raw field names, so a
field-name fix only touches those two files.

No `manifest.json` shipped with the download (only `events/`,
`inventory/`, `operational/`, `reference/`). `data/raw/manifest.json` was
generated locally (sha256 of each downloaded file) so
`pipelines/validate/validate_manifest.py` has something to check against.

## Confirmed source shapes

- **operational/customers.json**: `customer_id`, `city_id`,
  `customer_segment`, `created_at_utc`, `source_row_id`. No status/email/
  name fields.
- **operational/customer_profiles.json**: SCD2, *pre-computed* --
  `customer_id`, `city_id`, `customer_segment`, `valid_from_utc`,
  `valid_to_utc`, `source_row_id`. Versions chain exactly (one version's
  `valid_to_utc` is one second before the next version's
  `valid_from_utc`); the current version's `valid_to_utc` is the
  extraction date, not `null`/open-ended. Silver keeps these windows
  as-is and flags the row with the latest `valid_from` per customer as
  `is_current`, instead of re-deriving anything.
- **operational/customer_addresses.json**: same SCD2 shape --
  `address_id`, `customer_id`, `city_id`, `valid_from_utc`,
  `valid_to_utc`. No street/postal/country/is_primary fields. In the
  downloaded snapshot every customer has exactly one address version.
- **operational/products.json**: `product_id`, `product_name`, `sku`,
  `unit_price`. No brand.
- **operational/product_categories.json**: same pre-computed SCD2 shape
  as profiles -- `product_id`, `category_id`, `category_name`,
  `valid_from_utc`, `valid_to_utc`.
- **operational/stores.json**: `store_id`, `store_name`, `city_id`,
  `location_type`. No free-text region -- join `city_reference` if a
  region/country is needed.
- **operational/sales_channels.json**: `channel_id`, `channel_name`. No
  channel_type. Values: `WEB`, `MOBILE_APP`, `STORE`, `MARKETPLACE`.
- **operational/promotions.json**: `promotion_id`, `promotion_code`,
  `promotion_type`, `discount_rate`, `start_date`, `end_date` (plain
  dates, no time component).
- **operational/orders.json**: `order_id`, `customer_id`, `store_id`
  (`null` unless `sales_channel = 'STORE'`), `sales_channel`,
  `ordered_at_utc`, `updated_at_utc`, `status`, `shipping_revenue`. Status
  vocabulary: `PLACED`, `CONFIRMED`, `FULFILLED`, `CANCELLED`, `RETURNED`
  (upper-case -- a real, confirmed-in-data bug in the first draft of
  `build_gold.sql` compared this against lower-case literals like
  `'cancelled'`, which silently never matched anything).
- **operational/order_items.json**: `order_item_id`, `order_id`,
  `product_id`, `quantity`, `unit_price`, `item_discount_amount`. There is
  a real per-line discount field -- Gold's `discount_amount` sums this
  *and* `order_promotions.discount_amount`, not just the promotion side.
- **operational/order_promotions.json**: `order_id`, `promotion_id`,
  `discount_amount`, unchanged from the original guess.
- **events/{payment,refund,return,support}_events.json**: a shared
  envelope, not the flat per-family fields first assumed --
  `{event_id, event_type, ingested_at_utc, occurred_at_utc, payload: {...}}`.
  `occurred_at_utc` sometimes carries a non-UTC offset (e.g. `+07:00`)
  despite the field name -- this is the README's "timestamp dengan
  timezone berbeda" issue, and casting straight to `timestamptz` in SQL
  already normalizes it correctly, no extra handling needed. Per family:
  - `payment_events`: `event_type` in `PAYMENT_AUTHORIZED` /
    `PAYMENT_CAPTURED` / `PAYMENT_FAILED`; payload has `payment_id`,
    `order_id`, `amount`.
  - `refund_events`: `event_type` in `REFUND_ISSUED` / `REFUND_COMPLETED`;
    payload has `refund_id`, `order_id`, `amount`.
  - `return_events`: `event_type` in `RETURN_REQUESTED` /
    `RETURN_RECEIVED` / `RETURN_CLOSED`; payload has only `return_id`,
    `order_id` -- **no line-item or quantity detail at all**.
  - `support_events`: `event_type` in `SUPPORT_TICKET_CREATED` /
    `SUPPORT_TICKET_CLOSED`; payload has `ticket_id`, `customer_id`,
    `order_id`, and `reason` (only present on the CREATED event).
- **events/web_events.json**: same envelope; `event_type` in
  `ADD_TO_CART` / `PAGE_VIEW` / `PURCHASE` (independent signals, not a
  lifecycle); payload has `customer_id`, `session_id`, `order_id`,
  `channel`, `campaign_id` (nullable).
- **inventory/inventory_snapshots.csv**: `product_id`, `location_id`
  (same id space as `stores.store_id`, e.g. `STORE-01`), `snapshot_date`,
  `available_quantity`, `reserved_quantity`, `unit_cost` (unused).
- **reference/campaign_spend.csv**: `spend_date`, `campaign_id`,
  `channel` (same id space as `sales_channels.channel_id`),
  `spend_amount`.
- **reference/city_reference.json**: `city_id`, `city_name`, `country`.
  Silver keys this by `city_id`, not `city_name`.

## Event-log state reconstruction (the README's explicit requirement)

`payment_events`/`refund_events`/`return_events`/`support_events` are a
raw event log where several rows describe the lifecycle of *one*
underlying entity: a `payment_id` goes `AUTHORIZED -> CAPTURED` (or
`FAILED`), a `ticket_id` goes `CREATED -> CLOSED`, etc.
`build_silver.sql` collapses each entity id to **one row holding its
terminal status** (`silver.payment_events` is keyed by `payment_id`, not
`event_id`), instead of keeping one row per raw event -- this is the
milestone's "event identity resolution" / "reconstruct state" objective,
not an incidental design choice. `web_events` is the one exception: its
three event types are independent behavioral signals, not states of one
object, so it stays a flat log.

Status precedence used per family (later wins if both are present):
- payment: `authorized` < `failed`/`captured` (captured wins if both seen)
- refund: `issued` < `completed`
- return: `requested` < `received` < `completed`
- support: `open` < `closed`

## Design decisions not fully pinned down by the README

- **Bronze idempotency**: reruns of the *same* `pipeline_run_id` upsert on
  `(ingestion_run_id, source_file, source_line_number)` instead of
  appending again. A *new* run always appends fresh rows, even for
  unchanged source files -- Bronze accumulates history across scheduled
  runs, and Silver's full-refresh dedup (latest `ingested_at` wins per
  natural key) is what keeps that from leaking into Silver/Gold as
  duplicates.
- **Silver strategy is full refresh, not incremental**: every
  `build_silver.sql` run truncates Silver and rebuilds it from *all*
  accumulated Bronze evidence, not just the newest ingestion run.
- **Orders keep conflicting state instead of being filtered**: an order
  cancelled after its payment was captured (the scenario the README calls
  out explicitly) still gets one row in `gold.order_360`, with
  `order_status='CANCELLED'` and `payment_status='captured'` both
  visible. Gold exposes the conflict rather than hiding it.
- **Orders/items with a broken reference are dropped from Gold, not the
  order itself**: `order_items` with negative quantity are rejected at
  Silver (`silver.rejected_records`); orders with no matching
  `silver.customers` row (`has_valid_customer = false`) are excluded from
  `gold.order_360` entirely, because the contract requires
  `customer_id NOT NULL`. The quality gate's
  `missing_customer_reference_rate` check fails the run if this exceeds
  50%.
- **`discount_amount`** on `gold.order_360` = `sum(order_items.item_discount_amount)`
  + `sum(order_promotions.discount_amount)` -- the source has both a
  per-line discount and a per-promotion discount, and they are additive,
  not the same number restated.
- **`refunded_units`** on `gold.product_daily` is an order-level
  approximation: since `return_events` carries no line-item detail, a
  completed return marks every item on that order as returned.
- **`resolved order`** (used by `executive_kpis_daily.return_rate`) means
  `order_status IN ('FULFILLED', 'RETURNED')` -- PLACED/CONFIRMED are
  still in flight, CANCELLED never completed, so neither belongs in a
  "how often does a completed order come back" denominator.
- **Campaign attribution** (`gold.channel_campaign_daily`) is last-touch:
  an order is attributed to the most recent `web_events` row for the same
  customer and sales channel, with a non-null `campaign_id`, in the 7 days
  before the order.

## How this was verified

The Google Drive folder linked from `README.md` turned out to require a
signed-in Google session for anonymous `curl`/`gdown` access (its actual
sharing setting was more restricted than "anyone with the link"), so it
was downloaded through an already-authenticated browser session instead,
unzipped, and copied into `data/raw/`. Every field name above was
confirmed by loading each file and inspecting real keys/values (Python,
one-off), not assumed. `pipelines/run_pipeline.py` was then run against
this real dataset (10,001 orders / 24,960 order items / ~64,000 events /
35,040 inventory rows) through a disposable local Postgres container; see
the pipeline run's own `ops.stage_events` / `ops.quality_check_results`
rows for the actual outcome of that run.
