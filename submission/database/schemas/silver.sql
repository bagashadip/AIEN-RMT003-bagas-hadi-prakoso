CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejected_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_record_id TEXT,
    bronze_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    rejected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.sales_channels (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.stores (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    city_id TEXT,
    location_type TEXT,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.cities (
    city_id TEXT PRIMARY KEY,
    city_name TEXT,
    country TEXT,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    sku TEXT,
    unit_price NUMERIC(14,2),
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id TEXT PRIMARY KEY,
    promotion_code TEXT,
    promotion_type TEXT,
    discount_rate NUMERIC(6,4),
    start_date DATE,
    end_date DATE,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id TEXT PRIMARY KEY,
    city_id TEXT,
    customer_segment TEXT,
    created_at_utc TIMESTAMPTZ,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.customer_profiles (
    source_row_id TEXT PRIMARY KEY,
    customer_id TEXT,
    city_id TEXT,
    customer_segment TEXT,
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id TEXT PRIMARY KEY,
    customer_id TEXT,
    city_id TEXT,
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.product_categories (
    source_row_id TEXT PRIMARY KEY,
    product_id TEXT,
    category_id TEXT,
    category_name TEXT,
    valid_from_utc TIMESTAMPTZ,
    valid_to_utc TIMESTAMPTZ,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    store_id TEXT,
    sales_channel TEXT NOT NULL,
    status TEXT NOT NULL,
    ordered_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    shipping_revenue NUMERIC(14,2) NOT NULL,
    source_row_id TEXT,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14,2) NOT NULL,
    item_discount_amount NUMERIC(14,2) NOT NULL,
    is_known_product BOOLEAN NOT NULL,
    source_row_id TEXT,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.order_promotions (
    source_row_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    promotion_id TEXT NOT NULL,
    discount_amount	NUMERIC(14,2) NOT NULL,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.payment_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT	NOT NULL,
    payment_id TEXT,
    order_id TEXT,
    amount NUMERIC(14,2),
    occurred_at_utc	TIMESTAMPTZ	NOT NULL,
    ingested_at_utc	TIMESTAMPTZ	NOT NULL,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.refund_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT	NOT NULL,
    refund_id TEXT,
    order_id TEXT,
    amount NUMERIC(14,2),
    occurred_at_utc	TIMESTAMPTZ	NOT NULL,
    ingested_at_utc	TIMESTAMPTZ	NOT NULL,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.return_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT	NOT NULL,
    return_id TEXT,
    order_id TEXT,
    occurred_at_utc	TIMESTAMPTZ	NOT NULL,
    ingested_at_utc	TIMESTAMPTZ	NOT NULL,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.support_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT	NOT NULL,
    ticket_id TEXT,
    order_id TEXT,
    customer_id TEXT,
    reason TEXT,
    occurred_at_utc	TIMESTAMPTZ	NOT NULL,
    ingested_at_utc	TIMESTAMPTZ	NOT NULL,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT	NOT NULL,
    session_id TEXT,
    order_id TEXT,
    customer_id TEXT,
    channel TEXT,
    campaign_id TEXT,
    occurred_at_utc	TIMESTAMPTZ	NOT NULL,
    ingested_at_utc	TIMESTAMPTZ	NOT NULL,
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    product_id TEXT,
    location_id TEXT,
    snapshot_date DATE,
    available_quantity INTEGER,
    reserved_quantity INTEGER,
    unit_cost NUMERIC(14,2),
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL,
    PRIMARY KEY (product_id, location_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    spend_date DATE,
    campaign_id TEXT,
    channel TEXT,
    spend_amount NUMERIC(14,2),
    bronze_id BIGINT NOT NULL,
    pipeline_run_id	TEXT NOT NULL,
    PRIMARY KEY (spend_date, campaign_id, channel)
);