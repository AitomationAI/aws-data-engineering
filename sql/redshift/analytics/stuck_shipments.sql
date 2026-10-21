-- Open shipments with no tracking update for 48h+ - an actionable exception list.
SELECT
    s.shipment_id,
    c.carrier_name,
    s.current_status,
    s.last_location,
    s.last_event_ts,
    DATEDIFF(hour, s.last_event_ts, GETDATE()) AS hours_since_update
FROM logistics.shipments s
JOIN logistics.dim_carrier c ON c.carrier_code = s.carrier_code
WHERE s.is_delivered = 0
  AND s.last_event_ts < DATEADD(hour, -48, GETDATE())
ORDER BY hours_since_update DESC
LIMIT 100;
