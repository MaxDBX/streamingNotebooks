# Databricks notebook source
# MAGIC %md
# MAGIC #### Creating a static "item dimension" table
# MAGIC In this notebook a static dataframe is created with the following columns:
# MAGIC * item_id, which corresponds to item_id in the sales table
# MAGIC * category_id, id of the category that the item_id belongs to.
# MAGIC 
# MAGIC This dataframe is joined with the sales dataframe to demonstrate a stream-static join in the demo

# COMMAND ----------

# MAGIC %run ../includes/setup

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

item_num = 10000000
category_num = 100
item_dim = spark.range(1,item_num)
table_name = "sw_db.item_dim"

# COMMAND ----------

# MAGIC %sql -- Enable auto optimize/auto compaction for compacting small files (this is relevant for Topic 4 in the demo)
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

item_num = 10000000
category_num = 100
item_dim = spark.range(1,item_num).withColumnRenamed("id","item_id").withColumn("category_id", f.lit(1) + f.col("item_id") % category_num)

table_name = "sw_db.item_dim"
item_dim.write.format("delta").mode("overwrite").saveAsTable(table_name)