-- On-time delivery rate and average transit time per carrier.
SELECT
    c.carrier_name,
    c.sla_hours,
    COUNT(*)                                                        AS delivered_shipments,
    ROUND(AVG(s.transit_hours), 1)                                  AS avg_transit_hours,
    ROUND(100.0 * SUM(CASE WHEN s.transit_hours <= c.sla_hours THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                            AS on_time_pct,
    ROUND(100.0 * SUM(CASE WHEN s.exception_count > 0 THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                            AS exception_pct
FROM logistics.shipments s
JOIN logistics.dim_carrier c ON c.carrier_code = s.carrier_code
WHERE s.is_delivered = 1
GROUP BY c.carrier_name, c.sla_hours
ORDER BY on_time_pct DESC;
