# Databricks notebook source
##prod
from requests import request
import requests
import json

instance_id = 'xxxxxx.azuredatabricks.net'

api_version = '/api/2.1'
api_command = '/jobs/list'
url = f"https://{instance_id}{api_version}{api_command}"
#Adicionar secret
headers = {
  'Authorization': "Bearer xxxxxxx"
}

has_more = True
count = 0
offset = 0
jsonDataList = []
while has_more:
  params = {
    'expand_tasks': 'true',
    'offset': offset
  }

  response = requests.get(
    url = url,
    params = params,
    headers= headers
  )

  jsonDataList.append(json.dumps(json.loads(response.text), indent = 2))
  jsonRDD = sc.parallelize(jsonDataList)
  dfProd = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(jsonRDD)
  try:
    has_more = json.loads(response.text)['has_more']
  except:
    has_more = False
    
  count = count + 1
  offset = offset + 20
  print(count)
print(json.dumps(json.loads(response.text), indent = 2))

# COMMAND ----------

from pyspark.sql.functions import *
dfJobsProd = dfProd.select(explode("jobs").alias("jobs")).withColumn("environment", lit("PROD"))
dfJobsProd = dfJobsProd.withColumn('jobname',col('jobs.settings.name').cast('string'))
dfJobsProd.count()

# COMMAND ----------

dfJobsProd.select(
  dfJobsProd.environment.cast('string').alias("environment"),
  dfJobsProd.jobs.job_id.cast('string').alias("job_id"),
  dfJobsProd.jobs.creator_user_name.cast('string').alias("creator_user_name"),
  dfJobsProd.jobname,
  dfJobsProd.jobs.settings.schedule.cast('string').alias("schedule"),
  dfJobsProd.jobs.settings.schedule.quartz_cron_expression.cast('string').alias("quartz_cron_expression"),
  dfJobsProd.jobs.settings.email_notifications.cast('string').alias("email_notifications"),
  dfJobsProd.jobs.settings.timeout_seconds.cast('string').alias("timeout_seconds"),
  dfJobsProd.jobs.settings.max_concurrent_runs.cast('string').alias("max_concurrent_runs"),
  dfJobsProd.jobs.settings.tasks.cast('string').alias("tasks"),
  dfJobsProd.jobs.settings.format.cast('string').alias("format"),
  dfJobsProd.jobs.settings.tasks[0].existing_cluster_id.cast('string').alias("existing_cluster_id"),
  dfJobsProd.jobs.settings.tasks[1].existing_cluster_id.cast('string').alias("existing_cluster_id2"),
  dfJobsProd.jobs.settings.tasks[2].existing_cluster_id.cast('string').alias("existing_cluster_id3"),
  to_timestamp(dfJobsProd.jobs.created_time / 1000).alias('created_time')
).createOrReplaceTempView('vwJobs')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vwJobs
