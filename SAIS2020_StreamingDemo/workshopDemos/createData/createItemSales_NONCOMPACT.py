# Databricks notebook source
# MAGIC %md
# MAGIC #### Create a streaming dataframe that is NOT auto-optimised/compacted
# MAGIC This dataframe is used to demonstrate the read performande differende between auto-optimised and non auto-optimised Streaming Delta Table syncs.
# MAGIC 
# MAGIC **note**: Ensure that the Delta Table sync is roughly the same size as that of the auto compated/optimised sales table, to get a good performance comparison in Topic 4 in the demo.

# COMMAND ----------

# MAGIC %run ../includes/setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Auto Optimize to True

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false;

# COMMAND ----------

table_name = "sw_db.bronze_no_compact"
checkpointTable = checkpoint_location + table_name 

# COMMAND ----------

# MAGIC %md
# MAGIC #### From rate source create a "sales stream".
# MAGIC * 10 million records per second
# MAGIC * We assume 10 million distinct item_ids
# MAGIC * Each item id will have a time stamp and number of sales (ranging between 0 and 10)

# COMMAND ----------

rate = 10000000
item_nums = 10000000

ratePartitions = 40
df1 = (spark
       .readStream
       .format("rate")
       .option("rowsPerSecond", rate)
       .option("numPartitions",ratePartitions)
       .load())

table_name = "sw_db.bronze_no_compact"
checkpointTable = checkpoint_location + table_name 


df_sales = (df1
 .withColumn("item_id",f.col("value") % item_nums)
 .withColumn("sales", (f.lit(1) + 10 * f.rand(seed = 42)).cast("int"))
 .select("timestamp","item_id","sales")
)

# Write to Delta
(df_sales
 .writeStream
 .option("checkpointLocation", checkpointTable)
 .format("delta")
 .outputMode("append")
 .table(table_name)
)