-- Reference data: carriers and their contractual delivery SLA.
CREATE SCHEMA IF NOT EXISTS logistics;

CREATE TABLE IF NOT EXISTS logistics.dim_carrier (
    carrier_code   VARCHAR(10)   NOT NULL PRIMARY KEY,
    carrier_name   VARCHAR(100)  NOT NULL,
    service_level  VARCHAR(20)   NOT NULL,
    sla_hours      INTEGER       NOT NULL
)
DISTSTYLE ALL;

DELETE FROM logistics.dim_carrier;
INSERT INTO logistics.dim_carrier VALUES
    ('SWFT', 'Swift Parcel',        'express',  48),
    ('NRTH', 'Northline Freight',   'standard', 96),
    ('BLUE', 'Blue Arrow Logistics','standard', 72),
    ('PKGO', 'PackGo Couriers',     'economy',  120);
