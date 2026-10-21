-- ============================================================================
-- Redshift Serverless - logistics warehouse schema
-- Run once after `terraform apply` (Query Editor v2 or the Data API).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS logistics;

-- Landing zone for COPY; one Glue batch at a time per batch_id.
CREATE TABLE IF NOT EXISTS logistics.stg_shipment_events (
    event_id        VARCHAR(40)    NOT NULL,
    shipment_id     VARCHAR(40)    NOT NULL,
    carrier_code    VARCHAR(10)    NOT NULL,
    event_type      VARCHAR(20)    NOT NULL,
    event_ts        TIMESTAMP      NOT NULL,
    event_date      DATE           NOT NULL,
    location_code   VARCHAR(20),
    customer_id     VARCHAR(20),
    weight_kg       DECIMAL(10,2),
    declared_value  DECIMAL(12,2),
    is_terminal     BOOLEAN,
    source_file     VARCHAR(256),
    batch_id        CHAR(32)       NOT NULL,
    ingest_date     DATE           NOT NULL
)
DISTSTYLE EVEN;

-- Grain: one row per tracking event. Distributed on shipment_id so the
-- per-shipment rebuild is co-located; sorted by time for range scans.
CREATE TABLE IF NOT EXISTS logistics.fact_shipment_events (
    event_id        VARCHAR(40)    NOT NULL PRIMARY KEY,
    shipment_id     VARCHAR(40)    NOT NULL,
    carrier_code    VARCHAR(10)    NOT NULL REFERENCES logistics.dim_carrier (carrier_code),
    event_type      VARCHAR(20)    NOT NULL,
    event_ts        TIMESTAMP      NOT NULL,
    event_date      DATE           NOT NULL,
    location_code   VARCHAR(20),
    customer_id     VARCHAR(20),
    weight_kg       DECIMAL(10,2),
    declared_value  DECIMAL(12,2),
    is_terminal     BOOLEAN,
    batch_id        CHAR(32)       NOT NULL,
    loaded_at       TIMESTAMP      NOT NULL
)
DISTKEY (shipment_id)
COMPOUND SORTKEY (event_date, carrier_code);

-- Current state per shipment, rebuilt for every shipment a batch touches.
CREATE TABLE IF NOT EXISTS logistics.shipments (
    shipment_id      VARCHAR(40)   NOT NULL PRIMARY KEY,
    carrier_code     VARCHAR(10)   NOT NULL,
    customer_id      VARCHAR(20),
    origin_location  VARCHAR(20),
    last_location    VARCHAR(20),
    first_event_ts   TIMESTAMP,
    picked_up_ts     TIMESTAMP,
    delivered_ts     TIMESTAMP,
    last_event_ts    TIMESTAMP,
    current_status   VARCHAR(20),
    event_count      INTEGER,
    exception_count  INTEGER,
    transit_hours    DECIMAL(10,2),
    is_delivered     SMALLINT,
    refreshed_at     TIMESTAMP
)
DISTKEY (shipment_id)
SORTKEY (last_event_ts);

CREATE TABLE IF NOT EXISTS logistics.load_audit (
    batch_id     CHAR(32)   NOT NULL,
    ingest_date  DATE       NOT NULL,
    rows_staged  INTEGER    NOT NULL,
    loaded_at    TIMESTAMP  NOT NULL
)
DISTSTYLE ALL;
