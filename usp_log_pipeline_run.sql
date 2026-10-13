CREATE OR ALTER PROCEDURE gold.usp_log_pipeline_run
    @pipeline_run_id  VARCHAR(50),
    @run_label        VARCHAR(100),
    @trigger_time_utc DATETIME2(6),
    @silver_rows      INT
AS
BEGIN
    SET NOCOUNT ON;
    -- The PK is NOT ENFORCED, so the engine will NOT stop a duplicate. This guard does.
    IF NOT EXISTS (SELECT 1 FROM gold.etl_run_log WHERE pipeline_run_id = @pipeline_run_id)
        INSERT INTO gold.etl_run_log
            (pipeline_run_id, run_label, trigger_time_utc, silver_rows, logged_utc)
        VALUES
            (@pipeline_run_id, @run_label, @trigger_time_utc, @silver_rows, SYSUTCDATETIME());
END;
