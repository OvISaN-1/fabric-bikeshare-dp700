Open in : [Fabric] https://app.fabric.microsoft.com/groups/11ee1500-732a-40f3-93f6-de7d0d4c2b48/warehouses/76d192b0-77ef-46d6-bdad-211b0d164797?encodedSqlText=H4sIAAAAAAAACmWSwW7CMAyG75H6Dj6ChCrtPHEokIlO0E5pt2qnKGtSFrVNkRM28fZTWgpl5GL_lu3k_5Q1o1FOIWUQ7XLK4COmBRy6RoY_v9w64XRneCOcsi4gURaQIs63gMLUSkKUwSwgAAAZ3dF1DlU4jtTqvIAq_NK1sj5R7dGduezKupfXxDphpEDJb51DNuy9nCrU1r-mUeMwb7VthSu_faHtpK60kvzkSq-1OSjrLvpuEUsLnrzvV5TN5pB-UAazt4jlcR6nCaw-7w1AyjaUDeXpStjQbD337tEM219Yuh-oVaJ0vZdjp43rCZ6sb60CMg_IhZO8XqPlYqKMaJXXTmGrjWiuhUa4PnTGB225U-2xQ4Hnmz8caeM9bRxp4wNtfKCNE9LD4IQ0_iONd1gC0mO4_Q0MSJwklMFrGicDHqnb8Vf5DglpMrHvmS-HZ446IMWWMgoYooElPD3_AarsptGxAgAA , [SSMS] https://app.fabric.microsoft.com/groups/11ee1500-732a-40f3-93f6-de7d0d4c2b48/warehouses/76d192b0-77ef-46d6-bdad-211b0d164797?ssmsRedirect=1&server=jn54ys7wukpedelhkh2fsd4due-aak64ejkopzube7w3z6q2tblja.datawarehouse.fabric.microsoft.com&database=wh_gold&encodedSqlText=H4sIAAAAAAAACmWSwW7CMAyG75H6Dj6ChCrtPHEokIlO0E5pt2qnKGtSFrVNkRM28fZTWgpl5GL_lu3k_5Q1o1FOIWUQ7XLK4COmBRy6RoY_v9w64XRneCOcsi4gURaQIs63gMLUSkKUwSwgAAAZ3dF1DlU4jtTqvIAq_NK1sj5R7dGduezKupfXxDphpEDJb51DNuy9nCrU1r-mUeMwb7VthSu_faHtpK60kvzkSq-1OSjrLvpuEUsLnrzvV5TN5pB-UAazt4jlcR6nCaw-7w1AyjaUDeXpStjQbD337tEM219Yuh-oVaJ0vZdjp43rCZ6sb60CMg_IhZO8XqPlYqKMaJXXTmGrjWiuhUa4PnTGB225U-2xQ4Hnmz8caeM9bRxp4wNtfKCNE9LD4IQ0_iONd1gC0mO4_Q0MSJwklMFrGicDHqnb8Vf5DglpMrHvmS-HZ446IMWWMgoYooElPD3_AarsptGxAgAA
CREATE OR ALTER VIEW gold.vw_station_latest
AS
WITH ranked AS (
    SELECT f.station_key, f.bikes, f.empty_docks, f.docks, f.standard_bikes, f.ebikes,
           f.is_stale, f.dock_mismatch, f.modified_utc, f.ingested_utc,
           ROW_NUMBER() OVER (PARTITION BY f.station_key ORDER BY f.ingested_utc DESC) AS rn
    FROM gold.fact_bikepoint_status AS f
)
SELECT d.station_id, d.station_name, d.terminal_name, d.lat, d.lon, d.is_temporary,
       r.bikes, r.empty_docks, r.docks, r.standard_bikes, r.ebikes,
       r.is_stale, r.dock_mismatch, r.modified_utc, r.ingested_utc
FROM ranked AS r
INNER JOIN gold.dim_station AS d ON d.station_key = r.station_key
WHERE r.rn = 1;