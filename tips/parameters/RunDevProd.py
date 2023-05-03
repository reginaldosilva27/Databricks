# Databricks notebook source
# DBTITLE 1,1 - Variáveis de ambiente configuradas por Cluster
import os
environment = os.getenv("environment")
database = os.getenv("database")
storageroot = os.getenv("storageroot")

if environment == 'dev':
  print(environment)
  print(database)
  print(storageroot)

# Exemplo de utilização:
tbName = 'person'
path = f"{storageroot}\{database}\{tbName}"
print(path)
df.write.option("mergeSchema", "true") \
  .mode(f"append") \
  .format("delta") \
  .saveAsTable(f"{database}.{tbName}",path=path)

# COMMAND ----------

# DBTITLE 1,Recuperando Tag Default
spark.conf.get('spark.databricks.clusterUsageTags.clusterId')

# COMMAND ----------

# DBTITLE 1,Tag clusterAllTags
spark.conf.get('spark.databricks.clusterUsageTags.clusterAllTags')

# COMMAND ----------

# DBTITLE 1,2 - Azure Tags - Automaticamente adicionadas ao cluster
import json
## Essas tags só podem ser acessadas via clusterAllTags, diferente das Custom e Default
tags = json.loads(spark.conf.get('spark.databricks.clusterUsageTags.clusterAllTags'))
for tag in tags:
  if tag["key"] == 'storageroot':
    storageroot = tag["value"]
  if tag["key"] == 'databricks-environment':
    environment = tag["value"]
  if tag["key"] == 'department':
    department = tag["value"]
  if tag["key"] == 'company':
    company = tag["value"]

print(environment)
print(storageroot)
print(department)
print(company)

# COMMAND ----------

# DBTITLE 1,3 - Spark Conf - Fixando valor no notebook
workspace = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

if workspace == '5800865833021444': ##dev
  instance_id = f'adb-5800865833021444.4.azuredatabricks.net'
  storageroot='abfss://lakedev@storageaccountlake.dfs.core.windows.net'
  database='db_catalog_dev'
  environment='dev'
if workspace == '5800865833021442': ##prod
  instance_id = 'adb-5800865833021442.4.azuredatabricks.net' 
  storageroot='abfss://lakeprod@storageaccountlake.dfs.core.windows.net'
  database='db_catalog_prod'
  environment='prod'

print(environment)
print(storageroot)
print(database)
print(instance_id)

# COMMAND ----------

# DBTITLE 1,4 - Widgets
# https://www.datainaction.dev/post/databricks-parametrizando-seus-notebooks-like-a-boss-usando-widgets

# Definindo Widgets manualmente - Não é obrigatório, se você enviar via Job direto funciona
dbutils.widgets.text('environment',      '')
dbutils.widgets.text('storageroot',   '')
dbutils.widgets.text('database',   '')

# Pegando valor dos Widgets
environment =  dbutils.widgets.get('environment')
storageroot =  dbutils.widgets.get('storageroot')
database    =  dbutils.widgets.get('database')

print(environment)
print(storageroot)
print(database)
