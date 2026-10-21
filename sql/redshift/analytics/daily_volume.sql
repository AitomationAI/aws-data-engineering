-- Daily event volume by type - feeds the operations dashboard.
SELECT
    event_date,
    COUNT(DISTINCT shipment_id)                                         AS active_shipments,
    SUM(CASE WHEN event_type = 'PICKED_UP' THEN 1 ELSE 0 END)           AS pickups,
    SUM(CASE WHEN event_type = 'DELIVERED' THEN 1 ELSE 0 END)           AS deliveries,
    SUM(CASE WHEN event_type = 'EXCEPTION' THEN 1 ELSE 0 END)           AS exceptions
FROM logistics.fact_shipment_events
WHERE event_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY event_date
ORDER BY event_date;
