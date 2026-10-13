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

## Not built yet
Schedule and failure handling (in progress)
