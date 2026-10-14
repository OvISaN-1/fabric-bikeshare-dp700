# Bike share lakehouse and warehouse in Microsoft Fabric

Personal project built while preparing for the DP-700 exam.
Data: live TfL BikePoint API.

## Flow
TfL API > Copy activity > bronze lakehouse (raw JSON)
> PySpark notebook > silver lakehouse (typed Delta tables)
> T-SQL procedures > gold warehouse (star schema)

## Orchestration
One pipeline (`pl_gold_load_bikepoints`) runs the whole chain in order:
Invoke Pipeline (bronze ingest, wait on completion)
> Notebook (silver)
> Refresh SQL analytics endpoint
> usp_load_dim_station
> usp_load_fact_bikepoint_status
> usp_log_pipeline_run

- The pipeline parameter `p_run_label` goes to the notebook as a base parameter and to the run log.
- The notebook returns its row count as an exit value, and the log procedure stores it.
- The refresh runs before the gold procedures. Without it they can read a stale SQL endpoint.
- `etl_run_log` holds one row per run, keyed by the pipeline run ID.

## Design choices
- Snapshot fact table, grain = station + ingest time
- Watermark table for incremental loads
- MERGE for the station dimension (SCD type 1)
- Keys are NOT ENFORCED because the Warehouse does not enforce them, so procedures guard duplicates themselves

## Schedule
The pipeline runs every day at 08:00 London time (a fixed schedule). The schedule passes `p_run_label = scheduled-daily`, so scheduled runs are easy to find in `etl_run_log`. The first scheduled run, on 14 Oct, logged a trigger time of 07:00:10 UTC, which is 08:00 BST. The schedule ends on 16 Oct so it does not keep using trial capacity.

## Failure handling
- The notebook activity retries twice, 60 seconds apart. The refresh activity retries twice, 30 seconds apart. The refresh can fail while something is still writing to the lakehouse, so a retry usually clears it.
- The stored procedure activities do not retry. A procedure fails on a logic or permission error, and running it again only repeats the error.
- There is no failure email yet. Fabric only takes a work or school address and I have a personal one, so I check Monitor by hand.
- I tested the error paths on a separate pipeline, using a stored procedure that does not exist. With only an On failure path, the run Succeeded. With both On failure and On success drawn, the same failure made the run Failed.

## Not built yet
Streaming (Eventstream, Eventhouse), security, Git deployment and monitoring.
