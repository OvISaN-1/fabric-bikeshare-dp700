-- One row per pipeline run, written by the last activity in the pipeline.
-- Fabric Warehouse takes no inline primary key and no default constraints in CREATE TABLE.
-- DATETIME2 precision stops at 6.
CREATE TABLE gold.etl_run_log
(
    pipeline_run_id  VARCHAR(50)  NOT NULL,
    run_label        VARCHAR(100) NOT NULL,
    trigger_time_utc DATETIME2(6) NULL,
    silver_rows      INT          NULL,
    logged_utc       DATETIME2(6) NOT NULL
);

-- The key goes in after the table, as NONCLUSTERED and NOT ENFORCED. Key columns must be NOT NULL.
ALTER TABLE gold.etl_run_log
    ADD CONSTRAINT pk_etl_run_log PRIMARY KEY NONCLUSTERED (pipeline_run_id) NOT ENFORCED;
