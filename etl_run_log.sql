-- Run log: one row per pipeline run, written by the last pipeline activity.
-- Fabric Warehouse: no inline PK and no default constraints in CREATE TABLE.
-- DATETIME2 precision is capped at 6.
CREATE TABLE gold.etl_run_log
(
    pipeline_run_id  VARCHAR(50)  NOT NULL,
    run_label        VARCHAR(100) NOT NULL,
    trigger_time_utc DATETIME2(6) NULL,
    silver_rows      INT          NULL,
    logged_utc       DATETIME2(6) NOT NULL
);

-- Constraint after the table. NONCLUSTERED and NOT ENFORCED (PK columns must be NOT NULL).
ALTER TABLE gold.etl_run_log
    ADD CONSTRAINT pk_etl_run_log PRIMARY KEY NONCLUSTERED (pipeline_run_id) NOT ENFORCED;
