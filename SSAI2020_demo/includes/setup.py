# Databricks notebook source
# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC val userhome = s"dbfs:/tmp/$username"
# MAGIC 
# MAGIC spark.conf.set("com.databricks.tmp.username", username)
# MAGIC spark.conf.set("com.databricks.tmp.userhome", userhome)
# MAGIC 
# MAGIC display(Seq())

# COMMAND ----------

user_name = spark.conf.get("com.databricks.tmp.username")
user_home = spark.conf.get("com.databricks.tmp.userhome")

# COMMAND ----------

from pyspark.sql.streaming import StreamingQuery

def path_exists(path: str):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

def clean_workspace(path: str): 
  if path_exists(path):
    result = dbutils.fs.rm(path, True)
    if result == False:
      raise Exception("Unable to delete path: " + path)
      
def until_stream_is_ready(stream: StreamingQuery):
  while (stream.isActive and len(stream.recentProgress) == 0):
    pass # wait until there is any type of progress

  if stream.isActive:
    stream.awaitTermination(5)
    print("The stream is active and ready.")
  else:
    print("The stream is not active.")
  return stream # Return the stream so notebook works appropriately.

# COMMAND ----------

## Create Database in userhome location if it doesn't exist yet
db_location = "{}/streamingWorkshop/db".format(user_home)
db_name = "sw_db"
spark.sql("CREATE DATABASE IF NOT EXISTS {0} LOCATION {1}".format(db_name, "\"" + db_location + "\""))

checkpoint_location = db_location + "/checkpointTables/"