# Bike share lakehouse and warehouse in Microsoft Fabric

Personal project built while preparing for the DP-700 exam.
Data: live TfL BikePoint API.

## Flow
TfL API > Copy activity > bronze lakehouse (raw JSON)
> PySpark notebook > silver lakehouse (typed Delta tables)
> T-SQL procedures > gold warehouse (star schema)

## Design choices
- Snapshot fact table, grain = station + ingest time
- Watermark table for incremental loads
- MERGE for the station dimension (SCD type 1)
- Keys are NOT ENFORCED because the Warehouse does not enforce them

## Not built yet
Scheduled orchestration (in progress)
