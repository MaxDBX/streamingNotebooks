# Databricks notebook source
# MAGIC %md
# MAGIC #### item sales table
# MAGIC In this notebook a we use a streaming rate source to create a typical "sales" streaming Dataframe with:
# MAGIC * timestamp, time of the sales event
# MAGIC * item_id, item that was sold
# MAGIC * sales, number of items sold for that item_id

# COMMAND ----------

# MAGIC %run ../includes/setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Auto Optimize to True

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

# MAGIC %md
# MAGIC #### From rate source create a "sales stream".
# MAGIC * 10 million records per second
# MAGIC * We assume 10 million distinct item_ids
# MAGIC * Each item id will have a time stamp and number of sales (ranging between 0 and 10)

# COMMAND ----------

import pyspark.sql.functions as f

rate = 10000000
item_nums = 10000000

ratePartitions = 40
dfInput = (spark
       .readStream
       .format("rate")
       .option("rowsPerSecond", rate)
       .option("numPartitions",ratePartitions)
       .load())

dfSales = (dfInput
 .withColumn("item_id",f.col("value") % item_nums)
 .withColumn("sales", (f.lit(1) + 10 * f.rand(seed = 42)).cast("int"))
 .select("timestamp","item_id","sales")
)


# Define table name and checkpoint location of the streaming table. (checkpoint_location for database has been defined in setup notebook)
table_name = "sw_db.bronze_compact"
checkpointTable = checkpoint_location + table_name 

# Write to Delta
(dfSales
 .writeStream
 .option("checkpointLocation", checkpointTable)
 .format("delta")
 .outputMode("append")
 .table(table_name)
)