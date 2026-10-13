CREATE OR ALTER PROCEDURE gold.usp_load_fact_bikepoint_status
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @wm     DATETIME2(6);   -- last loaded batch
    DECLARE @new_wm DATETIME2(6);   -- newest batch available in silver

    SELECT @wm = last_ingested_utc
    FROM gold.etl_watermark
    WHERE table_name = 'fact_bikepoint_status';

    SELECT @new_wm = MAX(ingested_utc)
    FROM lh_silver.dbo.bikepoint_status
    WHERE ingested_utc > @wm;

    IF @new_wm IS NULL RETURN    -- nothing new, exit cleanly

    -- Keep the transaction short: one INSERT and one UPDATE.
    -- No savepoints or named transactions in Fabric Warehouse.
    BEGIN TRANSACTION

        INSERT INTO gold.fact_bikepoint_status
            (station_key, station_id, bikes, empty_docks, docks, standard_bikes, ebikes,
             is_installed, is_locked, is_stale, dock_mismatch, modified_utc, ingested_utc)
        SELECT d.station_key, f.station_id, f.bikes, f.empty_docks, f.docks, f.standard_bikes, f.ebikes,
               f.is_installed, f.is_locked, f.is_stale, f.dock_mismatch, f.modified_utc, f.ingested_utc
        FROM lh_silver.dbo.bikepoint_status AS f
        INNER JOIN gold.dim_station AS d ON d.station_id = f.station_id
        WHERE f.ingested_utc >  @wm
          AND f.ingested_utc <= @new_wm
          -- safety net: the PK is not enforced, so guard the grain (station_id + ingested_utc)
          AND NOT EXISTS (SELECT 1 FROM gold.fact_bikepoint_status AS x
                          WHERE x.station_id = f.station_id AND x.ingested_utc = f.ingested_utc);

        UPDATE gold.etl_watermark
        SET last_ingested_utc = @new_wm, updated_utc = SYSUTCDATETIME()
        WHERE table_name = 'fact_bikepoint_status';

    COMMIT TRANSACTION
END