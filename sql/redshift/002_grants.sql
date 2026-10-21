-- ============================================================================
-- The loader Lambda authenticates through the Data API with its IAM role,
-- which maps to the database user "IAMR:<role-name>". Grant it exactly what
-- the load needs. Replace the role name with the Terraform output value.
-- ============================================================================

GRANT USAGE ON SCHEMA logistics TO "IAMR:shipment-pipeline-dev-redshift-loader";

GRANT SELECT, INSERT, UPDATE, DELETE
    ON logistics.stg_shipment_events,
       logistics.fact_shipment_events,
       logistics.shipments,
       logistics.load_audit
    TO "IAMR:shipment-pipeline-dev-redshift-loader";

GRANT SELECT ON logistics.dim_carrier TO "IAMR:shipment-pipeline-dev-redshift-loader";

GRANT TEMP ON DATABASE logistics TO "IAMR:shipment-pipeline-dev-redshift-loader";
