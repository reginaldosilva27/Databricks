# Databricks notebook source
# DBTITLE 1,Criando uma ServicePrincipal e colocando em um grupo especifico
# MAGIC %sh
# MAGIC curl --netrc -X POST \
# MAGIC https://adb-4013955633331914.14.azuredatabricks.net/api/2.0/preview/scim/v2/ServicePrincipals \
# MAGIC --header 'Content-type: application/scim+json' \
# MAGIC --header 'Authorization: Bearer xxx-3' \
# MAGIC --data '{"schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],"applicationId": "96a9c13a-bd04-459f-a186-e36fe24b6c9a","displayName": "databricks-serviceprincipal","groups": [{"value": "612835559850353"}],"entitlements": [{ "value": "allow-cluster-create"}], "active": true}' 

# COMMAND ----------

# DBTITLE 1,Resgatando o GroupID
##prod
from requests import request
from pyspark.sql.functions import *
import requests
import json

instance_id = 'adb-4013955633331914.14.azuredatabricks.net'

api_version = '/api/2.0'
api_command = '/preview/scim/v2/Groups'
url = f"https://{instance_id}{api_version}{api_command}"

#Adicionar secret
headers = {
  'Authorization': "Bearer xxxx-3"
}

response = requests.get(
  url = url,
  headers=headers
)

jsonDataList = []
jsonDataList.append(json.dumps(json.loads(response.text), indent = 2))
jsonRDD = sc.parallelize(jsonDataList)
dfGroups = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(jsonRDD)
dfExplode = dfGroups.withColumn("Groups",explode(dfGroups.Resources))
dfExplode.select(dfExplode.Groups.id,dfExplode.Groups.displayName).display()

# COMMAND ----------

# DBTITLE 1,List ServicePrincipal
# MAGIC %sh
# MAGIC curl -X GET \
# MAGIC https://adb-4013955633331914.14.azuredatabricks.net/api/2.0/preview/scim/v2/ServicePrincipals \
# MAGIC --header "Authorization: Bearer xxxx-3" 

# COMMAND ----------

# DBTITLE 1,Genarate a short-live token - Use this token to generate a Databricks PAT for an app
# MAGIC %sh
# MAGIC curl -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
# MAGIC https://login.microsoftonline.com/[TenantID]/oauth2/v2.0/token \
# MAGIC -d 'client_id=96a9c13a-bd04-459f-a186-e36fe24b6c9a' \
# MAGIC -d 'grant_type=client_credentials' \
# MAGIC -d 'scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d%2F.default' \
# MAGIC -d 'client_secret=[App Secret]'

# COMMAND ----------

# DBTITLE 1,Create Token for ServicePrincipal - Use short-live token to authenticate
# MAGIC %sh
# MAGIC curl -X POST \
# MAGIC https://adb-4013955633331914.14.azuredatabricks.net/api/2.0/token/create \
# MAGIC --header "Content-type: application/json" \
# MAGIC --header "Authorization: Bearer [token]" \
# MAGIC --data '{"application_id": "96a9c13a-bd04-459f-a186-e36fe24b6c9a","comment": "Token para acesso no PowerBI, token não expira","lifetime_seconds": -1}'

# COMMAND ----------

# DBTITLE 1,Token List - Use App Token
##prod
from pyspark.sql.functions import *
from requests import request
import requests
import json

instance_id = 'adb-4013955633331914.14.azuredatabricks.net'

api_version = '/api/2.0'
api_command = '/token/list'
url = f"https://{instance_id}{api_version}{api_command}"

#Adicionar secret
headers = {
  'Authorization': "Bearer xxx-3"
}

response = requests.get(
  url = url,
  headers=headers
)

jsonDataList = []
jsonDataList.append(json.dumps(json.loads(response.text), indent = 2))
jsonRDD = sc.parallelize(jsonDataList)
dfGroups = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(jsonRDD)
print(json.dumps(json.loads(response.text), indent = 2))

# COMMAND ----------

# DBTITLE 1,Test Resquest API with ServicePrincipal Token 
# MAGIC %sh
# MAGIC curl -X GET \
# MAGIC -H 'Authorization: Bearer xxx-3' \
# MAGIC https://adb-4013955633331914.14.azuredatabricks.net/api/2.0/clusters/list
