# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run "./includes/setup"

# COMMAND ----------

# MAGIC %md
# MAGIC **Create utility functions for streaming output sinks**

# COMMAND ----------

import uuid
class NoopForeachWriter:
    def open(self, partition_id, epoch_id):
        # Open connection. This method is optional in Python.
        pass
    def process(self, row):
        # Write row to connection. This method is NOT optional in Python.
        pass
    def close(self, error):
        # Close the connection. This method in optional in Python.
        pass

def writeToNoop(df):
  checkpointLocation = f"{user_home}/{uuid.uuid4()}"
  (df
   .writeStream
   .option("checkpointLocation", checkpointLocation) # We do use a checkpoint location to simulate real scenarios.
   .foreach(NoopForeachWriter())
   .start())
  return checkpointLocation

def writeToForeachBatch(df, function_to_execute):
  checkpointLocation = f"{user_home}/{uuid.uuid4()}"
  (df
   .writeStream
   .option("checkpointLocation", checkpointLocation) # We do use a checkpoint location to simulate real scenarios.
   .foreachBatch(function_to_execute)
   .start())
  return checkpointLocation

# COMMAND ----------

# MAGIC %md
# MAGIC **Load in the static data frames and trigger cache**

# COMMAND ----------

# Load item_dim and item_dim_small dataframes
itemDF = spark.table("sw_db.item_dim").cache()
itemDF.write.format("noop").save() # Trigger the cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tuning offsets/files per trigger, and its relation to shuffle partitions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Introduction
# MAGIC In the first topic we will consider the tuning of input parameters. Specifically we consider the tuning of offsets/files per trigger, which dictates how large our mini batches will be. It is important to set these input parameters correctly in order to:
# MAGIC * Make sure our cluster is fully utilised in terms of CPU and memory
# MAGIC * Avoiding performance loss due to shuffle spill
# MAGIC * Obtain the optimal SQL query plan for our streaming pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions
# MAGIC We define the below function that constructs the input stream. It takes in a parameter "maxFilesPerTrigger" and a table name, and reads from a Delta file source

# COMMAND ----------

def constructInputStream(tableName, maxFilesPerTrigger = 1000):
  return (spark
          .readStream
          .option("maxFilesPerTrigger", maxFilesPerTrigger)
          .format("delta")
          .table(tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 1: Stream with larger number of files per trigger
# MAGIC In the first case we consider the case in which we set a large number of files per trigger. The size of each file in our input data source is around 200 MB in size. In the below example we consider:
# MAGIC * Setting number of shuffle partitions equal to number of workers.
# MAGIC * Setting filesPerTrigger to 20, (which is equal to ~4 GB on disk)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 20)
salesSDF = constructInputStream("sw_db.bronze_compact", maxFilesPerTrigger = 40)
itemSalesSDF = salesSDF.join(itemDF, "item_id")

writeToNoop(itemSalesSDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Shuffle Spill
# MAGIC * If we have a large number of files per trigger, we can get shuffle spill in the join step, in which the sort + shuffle occurs.
# MAGIC * Shuffle spill means that shuffle files are too large to be contained entirely in the memory of worker nodes.
# MAGIC * Workers nodes need to read/write shuffle files to disk as a result. This leads to sub-optimal use of your cluster.
# MAGIC * Results (filesPerTrigger = 40): 
# MAGIC   * Processing rate: 8 million records/second
# MAGIC   * Batch duration: 4.5 min.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 2: Stream with files per Trigger tuned
# MAGIC * We reduced the number of files per trigger, and as such have smaller shuffle partitions
# MAGIC * This means the join is much more efficient, since there is no longer any spill
# MAGIC * This in turn means a higher throughput will be observed.

# COMMAND ----------

# MAGIC %md
# MAGIC #### How to tune maxFilesPerTrigger
# MAGIC * Our starting point is our cluster size and number of shuffle partitions.
# MAGIC * **Rule of Thumb #1** number of shuffle partitions should be equal to the number of cores we have available in our cluster.
# MAGIC   * Reason for this is that this tends to give us the best trade off between latency and throughput.
# MAGIC * **Rule of Thumb #2**: You want "one shuffle partition" to be ~ 150-200 MB
# MAGIC   * This way we make optimal use of our cluster memory, while avoiding shuffle spill.
# MAGIC   * This assumes we are using a **memory optimized cluster , 8GB/core**
# MAGIC * So look at your Spark UI, and tune your files/offsets per trigger in such a way that you get optimal shuffle partitions.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 20)
salesSDF = constructInputStream("sw_db.bronze_compact", maxFilesPerTrigger = 6)
itemSalesSDF = salesSDF.join(itemDF, "item_id")
writeToNoop(itemSalesSDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary:
# MAGIC * 6 files/trigger + 20 shuffle partitions: ~10 million/second
# MAGIC * Batch duration: ~35 seconds

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 3: Broadcasting your static Table
# MAGIC * A big performance boost can be obtained by broadcasting our dimension table (itemDF).
# MAGIC * Broadcasting means we can make use of a broadcastHashJoin instead of a sortMergeJoin when joining `salesSDF` to `itemDF`.
# MAGIC * Enabling broadcasting leads to a throughput increase of 70%!
# MAGIC * The main take away here is to, whenever possible, broadcast your static DF.
# MAGIC  * If your static DF is too big, find ways to make it smaller to include only the records that are necessary
# MAGIC    * e.g. when you do a join on dates, make sure you only load in the LATEST dates.
# MAGIC * **Note** Because of this the number of input files also starts to matter less. It only really matters when we deal with shuffle partitions.
# MAGIC * **How should you now choose filesPerTrigger when making use of broadcast?**
# MAGIC  * The processing rate increases slightly for a larger files per tigger, but at the cost of higher latency.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 20)
salesSDF = constructInputStream("sw_db.bronze_compact", maxFilesPerTrigger = 6)
itemSalesSDF = salesSDF.join(itemDF.hint("broadcast"), "item_id")

writeToNoop(itemSalesSDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary
# MAGIC * 20 files/trigger, broadcasted item_dim: 17 M records/s, Batch duration: 1.1 minutes
# MAGIC * 40 files, broadcasted item_dim: 18.5 M records/s, Batch duration: 2.0 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 4: Broadcasting on the streaming DF.
# MAGIC * However, of course it is not always possible to have a small static DF. 
# MAGIC * Could we instead broadcast our streaming df mini-batch?
# MAGIC   * It turns out you can! You just have to make sure each mini-batch will be lower than the broadcast threshold.
# MAGIC * Does broadcasting the streaming part of the DF result in higher throughput?
# MAGIC   * In our case it turns out it's not. It's more efficient to do a bulk sort-merge-join instead of reducing files/trigger to make use of broad-casting.
# MAGIC   * We can only get 740K records/second, since we can only do 1 file per trigger to make sure we get a broadcast.
# MAGIC   * However there will be situations with lower records/second where it really does make sense to look into broadcasting the streaming side of the join.
# MAGIC   * Batch duration however is of course quite low at 6.1 sec, so we do have lower latency (simply because we process less data)
# MAGIC   * e.g. if you only have to reduce your mini-batch by, say, 50%, to get a broadcast on your mini-batch, definitely go for it!

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 20)
salesSDF = constructInputStream("sw_db.bronze_no_compact", maxFilesPerTrigger = 1)
enrichedSDF = salesSDF.hint("broadcast").join(itemDF, "item_id")

writeToNoop(enrichedSDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 2: Stateful parameters

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC #### Running our optimised pipeline so far.
# MAGIC We assume we have now correctly tuned our input parameters, i.e.:
# MAGIC * Set our shuffle parameters to 20.
# MAGIC * Set our number of files per trigger to 6.
# MAGIC   * We will introduce another shuffle step in this topic, and as such need our shuffle partitions to be small enough to avoid shuffle spill.
# MAGIC * Use broadcasting on static side of the join.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 20)
salesSDF = constructInputStream("sw_db.bronze_compact", maxFilesPerTrigger = 6) # Changed to 6 for lower latency and since we get shuffle.
itemSalesSDF = salesSDF.join(itemDF.hint("broadcast"), "item_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 1: No/Large Watermark
# MAGIC * When we make use of an extremely large watermark, we see that the "aggregation state" keeps increasing.
# MAGIC * It goes up by more than 6000 keys for each batch.
# MAGIC * In the short term this is not a problem, but at some point the number of distinct keys in the state-store becomes so large that it will start to hurt performance:
# MAGIC   * Both processing time and batch duration will go up.
# MAGIC * You can of course make use of RocksDB statestore, or by opting to use Delta as a state-store back-end instead, but this will only delay the inevitable.
# MAGIC * Because of this is it is important to use a watermark that's as short as possible.
# MAGIC * This is often a decision that is driven from your data and from your business:
# MAGIC   * **Data:** How often do you expect data to come "late", and by how much time do you expect it to be late?
# MAGIC   * **Business:** How problematic is it to miss a data point because it is late?

# COMMAND ----------

def aggregateSalesRevenue(df, watermarkLateness, timeWindowSize, aggregationKey):
  return (
    df
    .withWatermark("timestamp", watermarkLateness)
    .groupBy(
      f.window("timestamp", timeWindowSize),
      f.col(aggregationKey))
    .agg(f.sum(f.col("sales")).alias("sales")))

# COMMAND ----------

# Example of stream blowing up:
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
itemSalesPer10SecondsDF = aggregateSalesRevenue(itemSalesSDF, watermarkLateness = "1 year", timeWindowSize = "10 seconds", aggregationKey = "item_id")
writeToNoop(itemSalesPer10SecondsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary
# MAGIC * Going down from 8M rec/s to 2M rec/s to ultimately OOM.
# MAGIC * Distinct keys ever increasing is the cause.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 2: Using different state-store backend
# MAGIC Can we still use the same cluster configuration without comprimising on the size of the state?
# MAGIC * Using RocksDB-backed statestore (similar to the default option, but 100x more keys for similar memory/storage footprint)

# COMMAND ----------

# Use RocksDB as state store backend.
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
itemSalesPer10SecondsDF = aggregateSalesRevenue(itemSalesSDF, watermarkLateness = "1 year", timeWindowSize = "10 seconds", aggregationKey = "item_id")
writeToNoop(itemSalesPer10SecondsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary
# MAGIC * Starts at lower throughput.
# MAGIC * Taking much longer to slow down than default backend.
# MAGIC * Distinct keys keep increasing.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 3: "Normal" watermark
# MAGIC * When we apply a short watermark, we see that state-aggregation will not go up much over time.
# MAGIC * As such we will have a sustainable performance over time
# MAGIC   * Number of records processed per second and batch processing time will be consistent

# COMMAND ----------

spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
itemSalesPerMinuteSDF = aggregateSalesRevenue(itemSalesSDF, watermarkLateness = "5 minutes", timeWindowSize = "1 minute", aggregationKey = "item_id")
writeToNoop(itemSalesPerMinuteSDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary
# MAGIC * Going up to around 6M rec/s steady throughput.
# MAGIC * Distinct keys stabilize at 50M.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 4: Keeping your Stream State Store agnostic
# MAGIC 
# MAGIC Finally, we can also skip stateful operators entirely, and instead make use of a merge-operation on a Delta table to make use of existing counts.

# COMMAND ----------

def createDeltaBackedState(tableName, overwrite=False):
  
  from delta.tables import DeltaTable
  import pyspark.sql.types as T
  
  db_location = "dbfs:/home/carsten.thone@databricks.com/streamingWorkshop/db"
  db_table_name = "sw_db." + tableName
  checkpoint_location = db_location + "/checkpointTables/" + db_table_name 
  
  delta_schema = (
    T.StructType([
      T.StructField("item_id", T.LongType()),
      T.StructField("timestamp",T.TimestampType()),
      T.StructField("sales", T.LongType())
    ]))
  
  # Create an empty Delta table if it does not exist. This is required for the MERGE to work in the first mini batch.
  if overwrite or not DeltaTable.isDeltaTable(spark, db_location + "/" + db_table_name):   
    (spark
     .createDataFrame([], delta_schema)
     .write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .format("delta")
     .saveAsTable(db_table_name))
    spark.sql(f"ALTER TABLE {db_table_name} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = false)")

# COMMAND ----------

from delta.tables import *
from datetime import timedelta, datetime
def aggregateSalesRevenueDeltaBacked(updatesDF, epochId):
  
  # Sum up the new incoming keys
  incomingSalesAggregateDF = (
    updatesDF
    .withColumn("timestamp", f.date_trunc("minute", "timestamp"))
    .groupBy(
      f.col("timestamp"),
      f.col("item_id"))
    .agg(f.sum("sales").alias("sales")))
  
  targetTable = DeltaTable.forName(spark, "sw_db.delta_backed_state")
  # We merge the new sales with the already existing sales.
  # We simulate a watermark by only retrieving timestamp records greater than max seen timestamp - 5 minutes
  # Note that it is even better to partition the state by date if you have days worth of data, to skip over entire partitions,
  # when pushing down the predicate.
  mostRecentTimestamp = targetTable.toDF().select(f.max("timestamp").alias("max_timestamp")).head().max_timestamp
  watermarkTime = mostRecentTimestamp - timedelta(minutes=5) if mostRecentTimestamp else datetime.min
  
  (
    targetTable.alias("target").merge(
      incomingSalesAggregateDF.alias("source"),
       f"""
       target.item_id = source.item_id AND 
       target.timestamp = source.timestamp AND
       target.timestamp > cast('{watermarkTime}' AS TIMESTAMP) AND 
       source.timestamp > cast('{watermarkTime}' AS TIMESTAMP)
       """) 
    .whenMatchedUpdate(set = { "sales" : f.col("source.sales") + f.col("target.sales")}) 
    .whenNotMatchedInsertAll()
    .execute()
  )

# COMMAND ----------

createDeltaBackedState("delta_backed_state", overwrite=True)
writeToForeachBatch(itemSalesSDF, aggregateSalesRevenueDeltaBacked)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 4: Output parameters

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 1: Saving table without auto-compact
# MAGIC We are now going to take our stream and write it to a Delta sync. We will compare enabling auto-compact or not, and benchmark te results.  
# MAGIC * **Note**: For the purpose of this example, we use 1000 shuffle partitions, normally this would of course not be the case.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",1000)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
itemSalesPerMinuteSDF = aggregateSalesRevenue(itemSalesSDF, watermarkLateness = "5 minutes", timeWindowSize = "1 minute", aggregationKey = "item_id")
writeToNoop(itemSalesPerMinuteSDF)

# COMMAND ----------

def toDelta(df,outputTableName):
  (df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation",
           "dbfs:/Users/carsten.thone@databricks.com/streamingWorkshop/db/checkpointsTables/" + outputTableName)
   .table(outputTableName))

# COMMAND ----------

spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false")
spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false")

#toDelta(dfAgg, "sw_db.silver_no_compact")

# COMMAND ----------

df_no_compact = spark.table("sw_db.silver_no_compact")
df_no_compact.write.format("noop").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Case 2: Saving table with auto-compact
# MAGIC * We are now going to enable auto compaction, which will ensure files are compacted to a larger size before written to Delta.
# MAGIC * Since we now include an extra compaction step, processing speed should be slightly lower compared to the example without compaction. Essentially sacrifice some write speed for greatly increased read speed.

# COMMAND ----------

spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true")

#toDelta(dfAgg, "sw_db.silver_compact")

# COMMAND ----------

df_compact = spark.table("sw_db.silver_compact")
df_compact.write.format("noop").save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary: read from both tables and compare read speed
# MAGIC * We see that it takes 1.43 minutes to read from the non-compact table, while it takes only 5 seconds to read from the compacted table. Quite a significant difference!

# COMMAND ----------

# MAGIC %md
# MAGIC #### Appendix:
# MAGIC Adjust broadcast threshold if necessary

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",4294967296 * 3)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")