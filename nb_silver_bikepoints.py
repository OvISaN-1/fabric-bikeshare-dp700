#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_bikepoints
#
# Bronze JSON (TfL BikePoint) to typed, deduplicated silver Delta tables.

# In[1]:


# Sanity checks on the session
print(spark.version)                                 # expect 4.1.x (Runtime 2.0)
print(spark.conf.get("spark.sql.ansi.enabled"))      # false in this session: invalid casts return NULL, which is why silver uses try_cast
print(spark.conf.get("spark.sql.session.timeZone"))  # zone used to DISPLAY timestamps


# In[2]:


from pyspark.sql import functions as F, Window

# Relative path = default lakehouse (lh_silver), through the shortcut to lh_bronze.
# multiLine=True because the file is ONE JSON array, not one JSON object per line.
raw = spark.read.option("multiLine", True).json("Files/bikepoint/bikepoint_raw.json")

raw.printSchema()
print(raw.count())   # expect 799


# In[3]:


# explode = one row per station per property (like CROSS APPLY OPENJSON)
props = (raw
    .select("id", "commonName", "lat", "lon", F.explode("additionalProperties").alias("p"))
    .select("id", "commonName", "lat", "lon",
            F.col("p.key").alias("k"),
            F.col("p.value").alias("v"),
            F.col("p.modified").alias("modified")))

keys = ["TerminalName", "Installed", "Locked", "InstallDate", "RemovalDate", "Temporary",
        "NbBikes", "NbEmptyDocks", "NbDocks", "NbStandardBikes", "NbEBikes"]

# Pivot by conditional aggregation = MAX(CASE WHEN k = 'NbBikes' THEN v END) in T-SQL
wide = props.groupBy("id", "commonName", "lat", "lon").agg(
    *[F.max(F.when(F.col("k") == k, F.col("v"))).alias(k) for k in keys],
    F.max(F.when(F.col("k") == "NbBikes", F.col("modified"))).alias("modified_raw"))


# In[4]:


# try_cast = TRY_CAST: bad values become NULL instead of failing the job.
# All source values are strings, so every cast is explicit.
silver = wide.select(
    F.col("id").alias("station_id"),
    F.col("commonName").alias("station_name"),
    "lat", "lon",
    F.col("TerminalName").alias("terminal_name"),
    F.expr("try_cast(Installed as boolean)").alias("is_installed"),
    F.expr("try_cast(Locked as boolean)").alias("is_locked"),
    F.expr("try_cast(Temporary as boolean)").alias("is_temporary"),
    F.expr("try_cast(NbBikes as int)").alias("bikes"),
    F.expr("try_cast(NbEmptyDocks as int)").alias("empty_docks"),
    F.expr("try_cast(NbDocks as int)").alias("docks"),
    F.expr("try_cast(NbStandardBikes as int)").alias("standard_bikes"),
    F.expr("try_cast(NbEBikes as int)").alias("ebikes"),
    F.expr("timestamp_millis(try_cast(InstallDate as bigint))").alias("install_ts"),  # epoch ms to UTC timestamp
    F.expr("try_to_timestamp(modified_raw)").alias("modified_utc"))                   # source is ISO 8601 with Z (UTC)

# Dedup, deterministic: keep the latest row per station.
# dropDuplicates() would keep an ARBITRARY row. This is ROW_NUMBER() OVER (...) = 1 in T-SQL.
w = Window.partitionBy("station_id").orderBy(F.col("modified_utc").desc_nulls_last())
silver = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

# Flag data quality problems, do not delete the rows
ref = silver.agg(F.max("modified_utc")).first()[0]   # newest update in this snapshot
silver = (silver
    .withColumn("is_stale", F.col("modified_utc") < F.lit(ref) - F.expr("INTERVAL 30 MINUTES"))   # 30 min is my choice
    .withColumn("dock_mismatch", (F.col("bikes") + F.col("empty_docks")) != F.col("docks"))
    .withColumn("ingested_utc", F.current_timestamp()))


# In[5]:


# Split slow-changing station attributes from fast-changing status readings
stations = silver.select("station_id", "station_name", "lat", "lon", "terminal_name",
                         "install_ts", "is_temporary")
status = silver.select("station_id", "bikes", "empty_docks", "docks", "standard_bikes", "ebikes",
                       "is_installed", "is_locked", "modified_utc", "is_stale", "dock_mismatch", "ingested_utc")

# Managed Delta tables in the dbo schema of the default lakehouse.
# overwrite = full reload (atomic, the old version stays in Delta history). Incremental MERGE comes in Phase 3.
(stations.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable("dbo.bikepoint_stations"))
(status.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable("dbo.bikepoint_status"))


# In[6]:


spark.sql("""
SELECT COUNT(*)                         AS stations,
       SUM(bikes)                       AS total_bikes,
       SUM(CAST(is_stale AS INT))       AS stale,
       SUM(CAST(dock_mismatch AS INT))  AS mismatch
FROM dbo.bikepoint_status
""").show()


# In[7]:


# Verification cell: read the Delta table directly to tell lakehouse data from SQL endpoint lag
# 1. Direct read of the Delta table (skips the SQL endpoint, so no sync lag)
spark.sql("SELECT MAX(ingested_utc) AS silver_max_direct FROM dbo.bikepoint_status").show(truncate=False)

# 2. When was the table last written, and by what
spark.sql("DESCRIBE HISTORY dbo.bikepoint_status") \
     .select("version", "timestamp", "operation") \
     .show(5, truncate=False)


# In[8]:


# PARAMETERS CELL (toggled as a parameter cell in Fabric). Defaults only.
# At run time the pipeline injects a new cell right below this one that overwrites these values.
# T-SQL comparison: like a stored procedure parameter, but the caller sets it per run.
p_run_label = "manual"


# In[9]:


print(f"Run label: {p_run_label}")


# In[10]:


# Return a value to the pipeline. Exit values are ALWAYS strings.
# Keep it at top level. Inside try/except the exit does not take effect.
# Interactively this raises an exception on purpose and the session stays alive.
# silver.bikepoint_status is overwritten each run, so its count = rows of this batch
notebookutils.notebook.exit(str(spark.table("dbo.bikepoint_status").count()))

