-- E-Commerce Real-Time Streaming Ingestion Schema
-- Designed for high-throughput bulk upserts with ON CONFLICT idempotency

CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64),
    order_id VARCHAR(64),
    quantity INTEGER DEFAULT 1,
    amount NUMERIC(12, 2) DEFAULT 0.00,
    currency VARCHAR(3) DEFAULT 'USD',
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB
);

-- Optimize for time-series range queries and analytical aggregations
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_timestamp ON ecommerce_events (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ecommerce_events_type_user ON ecommerce_events (event_type, user_id);

-- Summary metrics view for real-time monitoring
CREATE OR REPLACE VIEW v_streaming_kpis AS
SELECT
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    COALESCE(SUM(amount), 0) AS total_gross_merchandise_value,
    MIN(event_timestamp) AS earliest_event,
    MAX(event_timestamp) AS latest_event,
    MAX(ingested_at) - MIN(ingested_at) AS ingestion_duration
FROM ecommerce_events
GROUP BY event_type;

