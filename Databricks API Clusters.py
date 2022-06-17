# Databricks notebook source
import requests
import json

instance_id = 'adb-xxxxxxx.xx.azuredatabricks.net' ##put your databricks host here

api_version = '/api/2.0'
api_command = '/clusters/list'
url = f"https://{instance_id}{api_version}{api_command}"

headers = {
  'Authorization': "Bearer xxxxxx"  ## put your databricks token here
}

response = requests.get(
  url = url,
  headers=headers
)

jsonDataList = []
jsonDataList.append(json.dumps(json.loads(response.text), indent = 2))
jsonRDD = sc.parallelize(jsonDataList)
df = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(jsonRDD)
print(json.dumps(json.loads(response.text), indent = 2))

# COMMAND ----------

from pyspark.sql.functions import *
dfclusters = df.select(explode("clusters").alias("clusters"))

# COMMAND ----------

dfclusters.select(
  dfclusters.clusters.cluster_id,
  dfclusters.clusters.cluster_name,
  dfclusters.clusters.cluster_cores,
  dfclusters.clusters.cluster_memory_mb,
  dfclusters.clusters.cluster_log_status,
  dfclusters.clusters.cluster_log_conf,
  dfclusters.clusters.cluster_source.alias("cluster_source"),
  dfclusters.clusters.creator_user_name,
  dfclusters.clusters.autotermination_minutes,
  dfclusters.clusters.azure_attributes,
  dfclusters.clusters.autoscale,
  dfclusters.clusters.custom_tags,
  dfclusters.clusters.default_tags,
  dfclusters.clusters.driver,
  dfclusters.clusters.driver_instance_source,
  dfclusters.clusters.driver_node_type_id,
  dfclusters.clusters.node_type_id,
  dfclusters.clusters.effective_spark_version.alias("effective_spark_version"),
  dfclusters.clusters.enable_elastic_disk,
  dfclusters.clusters.executors,
  dfclusters.clusters.last_restarted_time,
  dfclusters.clusters.last_state_loss_time,
  dfclusters.clusters.num_workers,
  dfclusters.clusters.runtime_engine.alias("runtime_engine"),
  dfclusters.clusters.spark_conf,
  dfclusters.clusters.start_time,
  dfclusters.clusters.state,
  dfclusters.clusters.state_message,
  dfclusters.clusters.terminated_time,
  dfclusters.clusters.termination_reason
).createOrReplaceTempView('vw_clusters')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_clusters

# COMMAND ----------

# MAGIC %sql
# MAGIC select cluster_source, count(*) from vw_clusters
# MAGIC group by cluster_source

# COMMAND ----------

# MAGIC %sql
# MAGIC select runtime_engine, count(*) from vw_clusters
# MAGIC group by runtime_engine
