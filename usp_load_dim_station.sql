CREATE OR ALTER PROCEDURE gold.usp_load_dim_station
AS
BEGIN
    SET NOCOUNT ON;

    -- Fabric MERGE: semicolon is mandatory at the end, INSERT takes VALUES only
    -- Columns are nullable, so <> alone would miss NULL changes: ISNULL with a sentinel
    MERGE gold.dim_station AS tgt
    USING (
        SELECT station_id, station_name, terminal_name,
               CAST(lat AS DECIMAL(9,6)) AS lat,
               CAST(lon AS DECIMAL(9,6)) AS lon,
               install_ts, is_temporary
        FROM lh_silver.dbo.bikepoint_stations
    ) AS src
    ON tgt.station_id = src.station_id
    WHEN MATCHED AND (
           ISNULL(tgt.station_name,  '') <> ISNULL(src.station_name,  '')
        OR ISNULL(tgt.terminal_name, '') <> ISNULL(src.terminal_name, '')
        OR ISNULL(tgt.lat, -999)         <> ISNULL(src.lat, -999)
        OR ISNULL(tgt.lon, -999)         <> ISNULL(src.lon, -999)
        OR ISNULL(tgt.install_ts, '1900-01-01') <> ISNULL(src.install_ts, '1900-01-01')
        OR ISNULL(CAST(tgt.is_temporary AS INT), -1) <> ISNULL(CAST(src.is_temporary AS INT), -1)
    )
    THEN UPDATE SET
        station_name  = src.station_name,
        terminal_name = src.terminal_name,
        lat           = src.lat,
        lon           = src.lon,
        install_ts    = src.install_ts,
        is_temporary  = src.is_temporary,
        dw_loaded_utc = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (station_id, station_name, terminal_name, lat, lon, install_ts, is_temporary, dw_loaded_utc)
         VALUES (src.station_id, src.station_name, src.terminal_name, src.lat, src.lon,
                 src.install_ts, src.is_temporary, SYSUTCDATETIME());
END