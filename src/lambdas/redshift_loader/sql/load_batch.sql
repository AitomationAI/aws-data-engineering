-- ============================================================================
-- Load one curated Glue batch into Redshift. Runs as a single Data API batch
-- (= one transaction). Placeholders are rendered by the loader Lambda.
-- ============================================================================

-- 1. Stage the batch straight from Parquet in S3
DELETE FROM logistics.stg_shipment_events WHERE batch_id = '{{ batch_id }}';

COPY logistics.stg_shipment_events (
    event_id, shipment_id, carrier_code, event_type, event_ts, event_date,
    location_code, customer_id, weight_kg, declared_value, is_terminal,
    source_file, batch_id, ingest_date
)
FROM '{{ s3_path }}'
IAM_ROLE '{{ iam_role }}'
FORMAT AS PARQUET;

-- 2. Isolate this batch (MERGE needs a table source; staging may hold
--    other concurrent batches). Temp tables live for the Data API session.
CREATE TEMP TABLE batch_events AS
SELECT * FROM logistics.stg_shipment_events WHERE batch_id = '{{ batch_id }}';

-- 3. Upsert events (re-delivered events overwrite, new events insert)
MERGE INTO logistics.fact_shipment_events
USING batch_events s
ON logistics.fact_shipment_events.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
    shipment_id    = s.shipment_id,
    carrier_code   = s.carrier_code,
    event_type     = s.event_type,
    event_ts       = s.event_ts,
    event_date     = s.event_date,
    location_code  = s.location_code,
    customer_id    = s.customer_id,
    weight_kg      = s.weight_kg,
    declared_value = s.declared_value,
    is_terminal    = s.is_terminal,
    batch_id       = s.batch_id,
    loaded_at      = GETDATE()
WHEN NOT MATCHED THEN INSERT (
    event_id, shipment_id, carrier_code, event_type, event_ts, event_date,
    location_code, customer_id, weight_kg, declared_value, is_terminal,
    batch_id, loaded_at
) VALUES (
    s.event_id, s.shipment_id, s.carrier_code, s.event_type, s.event_ts, s.event_date,
    s.location_code, s.customer_id, s.weight_kg, s.declared_value, s.is_terminal,
    s.batch_id, GETDATE()
);

-- 4. Rebuild the current-state snapshot for every shipment touched by this batch
DELETE FROM logistics.shipments
WHERE shipment_id IN (
    SELECT DISTINCT shipment_id FROM batch_events
);

INSERT INTO logistics.shipments (
    shipment_id, carrier_code, customer_id, origin_location, last_location,
    first_event_ts, picked_up_ts, delivered_ts, last_event_ts, current_status,
    event_count, exception_count, transit_hours, is_delivered, refreshed_at
)
SELECT
    e.shipment_id,
    MAX(e.carrier_code),
    MAX(e.customer_id),
    MAX(CASE WHEN e.event_type = 'PICKED_UP' THEN e.location_code END),
    MAX(CASE WHEN e.event_ts = l.last_ts THEN e.location_code END),
    MIN(e.event_ts),
    MIN(CASE WHEN e.event_type = 'PICKED_UP' THEN e.event_ts END),
    MIN(CASE WHEN e.event_type = 'DELIVERED' THEN e.event_ts END),
    MAX(e.event_ts),
    MAX(CASE WHEN e.event_ts = l.last_ts THEN e.event_type END),
    COUNT(*),
    SUM(CASE WHEN e.event_type = 'EXCEPTION' THEN 1 ELSE 0 END),
    DATEDIFF(minute,
             MIN(CASE WHEN e.event_type = 'PICKED_UP' THEN e.event_ts END),
             MIN(CASE WHEN e.event_type = 'DELIVERED' THEN e.event_ts END)) / 60.0,
    MAX(CASE WHEN e.event_type = 'DELIVERED' THEN 1 ELSE 0 END),
    GETDATE()
FROM logistics.fact_shipment_events e
JOIN (
    SELECT shipment_id, MAX(event_ts) AS last_ts
    FROM logistics.fact_shipment_events
    GROUP BY shipment_id
) l ON l.shipment_id = e.shipment_id
WHERE e.shipment_id IN (
    SELECT DISTINCT shipment_id FROM batch_events
)
GROUP BY e.shipment_id;

-- 5. Audit + clean up staging
INSERT INTO logistics.load_audit (batch_id, ingest_date, rows_staged, loaded_at)
SELECT '{{ batch_id }}', '{{ ingest_date }}', COUNT(*), GETDATE()
FROM batch_events;

DELETE FROM logistics.stg_shipment_events WHERE batch_id = '{{ batch_id }}';
