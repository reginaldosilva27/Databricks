# Databricks notebook source
# DBTITLE 1,Listando todos os clusters Databricks
import requests
import json

## O seu databricks Instance voce encontra na sua URL.
instance_id = 'xxxxxxx.azuredatabricks.net' 

# Aqui estamos usando a API na versão 2.0 que é a mais recente no momento
api_version = '/api/2.0'
api_command = '/clusters/list'
url = f"https://{instance_id}{api_version}{api_command}"
print(url)

headers = {
  'Authorization': "Bearer xxxxxxx"  ## put your databricks token here
}

response = requests.get(
  url = url,
  headers=headers
)

# Transformando nosso retorno em um Dataframe
jsonDataList = []
jsonDataList.append(json.dumps(json.loads(response.text), indent = 2))
jsonRDD = sc.parallelize(jsonDataList)
df = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(jsonRDD)

# COMMAND ----------

# DBTITLE 1,Print em modo Text
print(response.text)

# COMMAND ----------

# DBTITLE 1,Print bonito
print(json.dumps(json.loads(response.text), indent = 2))

# COMMAND ----------

# DBTITLE 1,Expandindo itens
from pyspark.sql.functions import *
# Usando o Explode para expandir nosso Json.
dfclusters = df.select(explode("clusters").alias("cl"))
dfclusters.display()

# COMMAND ----------

# DBTITLE 1,Selecionando os campos relevantes
dfclusters.select(
  dfclusters.cl.cluster_id,
  dfclusters.cl.cluster_name,
  dfclusters.cl.cluster_cores,
  dfclusters.cl.cluster_memory_mb,
  dfclusters.cl.state,
  dfclusters.cl.spark_conf,
  dfclusters.cl.cluster_source.alias("cluster_source"),
  dfclusters.cl.creator_user_name,
  dfclusters.cl.autotermination_minutes,
  dfclusters.cl.azure_attributes,
  dfclusters.cl.autoscale,
  dfclusters.cl.custom_tags,
  dfclusters.cl.default_tags,
  dfclusters.cl.driver,
  dfclusters.cl.driver_instance_source,
  dfclusters.cl.driver_node_type_id,
  dfclusters.cl.node_type_id,
  dfclusters.cl.effective_spark_version.alias("effective_spark_version"),
  dfclusters.cl.enable_elastic_disk,
  dfclusters.cl.last_restarted_time,
  dfclusters.cl.last_state_loss_time,
  dfclusters.cl.num_workers,
  dfclusters.cl.runtime_engine.alias("runtime_engine"),
  dfclusters.cl.spark_conf,
  dfclusters.cl.start_time,
  dfclusters.cl.state,
  dfclusters.cl.state_message,
  dfclusters.cl.terminated_time,
  dfclusters.cl.termination_reason
).createOrReplaceTempView('vw_clusters')

# COMMAND ----------

# DBTITLE 1,Consultando com SQL
# MAGIC %sql
# MAGIC -- Para os amantes de SQL
# MAGIC select * from vw_clusters

# COMMAND ----------

# DBTITLE 1,Agrupando por versão e origem
# MAGIC %sql
# MAGIC select cluster_source,effective_spark_version,count(*) as qtdClusters from vw_clusters
# MAGIC group by cluster_source,effective_spark_version
# MAGIC order by cluster_source,effective_spark_version

# COMMAND ----------

# DBTITLE 1,Criando um novo cluster via API
# Aqui estamos usando a API na versão 2.0 que é a mais recente no momento
api_version = '/api/2.0'
api_command = '/clusters/create'
url = f"https://{instance_id}{api_version}{api_command}"
print(url)

headers = {
  'Authorization': "Bearer xxxxxx-2"  ## put your databricks token here
}

datajson = {
  "cluster_name": "my-cluster-api",
  "spark_version": "11.3.x-scala2.12",
  "node_type_id": "Standard_D3_v2",
  "spark_conf": {
    "spark.speculation": True
  },
  "num_workers": 1
}

print(json.dumps(datajson, indent = 2))
data = json.dumps(datajson, indent = 2)
response = requests.post(url = url, headers = headers, data = data)
print(response)

# COMMAND ----------

# DBTITLE 1,Deletando um cluster via API
# Aqui estamos usando a API na versão 2.0 que é a mais recente no momento
api_version = '/api/2.0'
api_command = '/clusters/delete'
url = f"https://{instance_id}{api_version}{api_command}"
print(url)

headers = {
  'Authorization': "Bearer xxxxxx"  ## put your databricks token here
}

datajson = {"cluster_id": "0211-131904-kvyksq3e"}

print(json.dumps(datajson, indent = 2))
data = json.dumps(datajson, indent = 2)
response = requests.post(url = url, headers = headers, data = data)
print(response)

# COMMAND ----------

# DBTITLE 1,Salvando como tabela Delta
## Adicione o caminho do storage
caminhoDatalakeLog = '[]'
df = spark.sql("select * from vw_clusters")
df.write.option("mergeSchema", "true").mode(f"overwrite").format("delta").save(f"{caminhoDatalakeLog}")
spark.sql(f"Create Table if not exists [nome do seu banco de dados].monitoramento_clusters Using Delta Location '{caminhoDatalakeLog}'")
