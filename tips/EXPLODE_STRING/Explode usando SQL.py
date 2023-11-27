# Databricks notebook source
# DBTITLE 1,Listando Grupos de logins no Databricks via API
import requests
import json

# Define a URL base da API do Databricks
instance_id = 'adb-47319640954053.13.azuredatabricks.net'

api_version = '/api/2.0'
api_command = '/preview/scim/v2/Groups'
url_list = f"https://{instance_id}{api_version}{api_command}"
url_list_members = f"https://{instance_id}/api/2.0/preview/scim/v2/Groups/"

print(url_list)

# Define o cabeçalho com o token de autenticação do Databricks
headers = {
    'Authorization': "Bearer xxx-3",
    "Content-Type": "application/json"
}
              
has_more = True
count = 0
offset = 0
jsonGroups = []
while has_more:
  params = {
    'expand_tasks': 'true',
    'offset': offset
  }
  try:
      print('Listando grupos')
      responseList = requests.get(
        url = url_list,
        params = params,
        headers= headers
    )
  except Exception as error:
      print(error)
      
  jsonGroups.append(responseList.json())
  try:
    has_more = json.loads(responseList.text)['has_more']
  except:
    has_more = False
    
  count = count + 1
  offset = offset + 20
  
print(jsonGroups)

# COMMAND ----------

# DBTITLE 1,Transformando a lista em Dataframe
jsonRDD = sc.parallelize(jsonGroups)
df = spark.read.option('multiline', 'true').option('inferSchema', 'true').json(jsonRDD)
df.createOrReplaceTempView('vw_sql_temp')

# COMMAND ----------

# DBTITLE 1,Criando o mesmo Dataframe usando tudo como String
jsonValues = []
jsonValues.append({'totalResults': 7, 'startIndex': 1, 'itemsPerPage': 7, 'schemas': ["urn:ietf:params:scim:api:messages: 2.0:ListResponse"], 'Resources': "[{'displayName': 'read_write_prod', 'entitlements': null, 'groups': [{'$ref': 'Groups/769409655224333', 'display': 'read_dev', 'type': 'direct', 'value': '769409655224333'}], 'id': '67674397758141', 'members': [{'$ref': 'Users/8675301566931963', 'display': 'reginaldo.silva@dataside.com.br', 'value': '8675301566931963'}, {'$ref': 'ServicePrincipals/2955041608089028', 'display': 'databricks-serviceprincipal', 'value': '2955041608089028'}, {'$ref': 'Users/547673826594172', 'display': 'Reginaldo Silva', 'value': '547673826594172'}, {'$ref': 'Users/1581289963735043', 'display': 'Regis Naldo', 'value': '1581289963735043'}, {'$ref': 'Users/1948297106640748', 'display': 'Reginaldo Silva', 'value': '1948297106640748'}], 'meta': {'resourceType': 'Group'}}, {'displayName': 'read_prod', 'entitlements': null, 'groups': [{'$ref': 'Groups/766964445608499', 'display': 'read_write_dev', 'type': 'direct', 'value': '766964445608499'}], 'id': '138152945819756', 'members': [{'$ref': 'Users/8675301566931963', 'display': 'reginaldo.silva@dataside.com.br', 'value': '8675301566931963'}], 'meta': {'resourceType': 'Group'}}, {'displayName': 'users', 'entitlements': [{'value': 'workspace-access'}, {'value': 'databricks-sql-access'}], 'groups': [], 'id': '371637887295750', 'members': [{'$ref': 'ServicePrincipals/2955041608089028', 'display': 'databricks-serviceprincipal', 'value': '2955041608089028'}, {'$ref': 'Users/8675301566931963', 'display': 'reginaldo.silva@dataside.com.br', 'value': '8675301566931963'}, {'$ref': 'Users/547673826594172', 'display': 'Reginaldo Silva', 'value': '547673826594172'}, {'$ref': 'Users/1581289963735043', 'display': 'Regis Naldo', 'value': '1581289963735043'}, {'$ref': 'Users/1948297106640748', 'display': 'Reginaldo Silva', 'value': '1948297106640748'}], 'meta': {'resourceType': 'WorkspaceGroup'}}, {'displayName': 'read_write_dev', 'entitlements': [{'value': 'databricks-sql-access'}], 'groups': [], 'id': '766964445608499', 'members': [{'$ref': 'Users/8675301566931963', 'display': 'reginaldo.silva@dataside.com.br', 'value': '8675301566931963'}, {'$ref': 'Users/1948297106640748', 'display': 'Reginaldo Silva', 'value': '1948297106640748'}, {'$ref': 'Groups/138152945819756', 'display': 'read_prod', 'value': '138152945819756'}], 'meta': {'resourceType': 'Group'}}, {'displayName': 'read_dev', 'entitlements': null, 'groups': [], 'id': '769409655224333', 'members': [{'$ref': 'Groups/67674397758141', 'display': 'read_write_prod', 'value': '67674397758141'}], 'meta': {'resourceType': 'Group'}}, {'displayName': 'admins', 'entitlements': [{'value': 'workspace-access'}, {'value': 'databricks-sql-access'}, {'value': 'allow-cluster-create'}, {'value': 'allow-instance-pool-create'}], 'groups': [], 'id': '868174163364744', 'members': [{'$ref': 'Users/547673826594172', 'display': 'Reginaldo Silva', 'value': '547673826594172'}, {'$ref': 'Users/1948297106640748', 'display': 'Reginaldo Silva', 'value': '1948297106640748'}], 'meta': {'resourceType': 'WorkspaceGroup'}}, {'displayName': 'demogroup', 'entitlements': null, 'groups': [], 'id': '1053327257318900', 'members': [{'$ref': 'Users/547673826594172', 'display': 'Reginaldo Silva', 'value': '547673826594172'}, {'$ref': 'Users/1581289963735043', 'display': 'Regis Naldo', 'value': '1581289963735043'}, {'$ref': 'Users/8675301566931963', 'display': 'reginaldo.silva@dataside.com.br', 'value': '8675301566931963'}, {'$ref': 'Users/1948297106640748', 'display': 'Reginaldo Silva', 'value': '1948297106640748'}], 'meta': {'resourceType': 'Group'}}]" })

from pyspark.sql.types import *
schema = StructType([
  StructField('Resources', StringType(), True),
  StructField('itemsPerPage', StringType(), True),
  StructField('schemas', StringType(), True),
  StructField('startIndex', StringType(), True),
  StructField('totalResults', StringType(), True)
  ])
  
dfString = spark.createDataFrame(jsonValues,schema)
dfString.createOrReplaceTempView('vw_sql_temp_string')
dfString.printSchema()

# COMMAND ----------

# DBTITLE 1,Dataframe tipado Array e Struct
# MAGIC %sql
# MAGIC select * from vw_sql_temp

# COMMAND ----------

# DBTITLE 1,Dataframe usando String
# MAGIC %sql
# MAGIC select * from vw_sql_temp_string

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Acessando um item especifico do Array
# MAGIC select Resources[3] from vw_sql_temp

# COMMAND ----------

# DBTITLE 1,Acessando um campo Array\Struct
# MAGIC %sql
# MAGIC -- Voce pode navegar de forma bem simples usando ponto
# MAGIC select Resources[3].displayName from vw_sql_temp

# COMMAND ----------

# DBTITLE 1,Explodindo e acessando niveis de forma simples
# MAGIC %sql
# MAGIC -- Listando todos os usuários e grupos a nivel de linha
# MAGIC -- Note que estamos acessando os campos apenas com . apos aplocar o Explode da coluna Resources
# MAGIC select Resources.displayName,explode(Resources.members.display) from( 
# MAGIC select explode(Resources) as Resources from vw_sql_temp
# MAGIC ) nivel1

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_json(Resources,'a string'),* from vw_sql_temp

# COMMAND ----------

# DBTITLE 1,Acessando campos do JSON no formato string
# MAGIC %sql
# MAGIC -- Ops, não é tão simples assim
# MAGIC select Resources.displayName,* from vw_sql_temp_string

# COMMAND ----------

# DBTITLE 1,FROM_JSON
# MAGIC %sql
# MAGIC select from_json(Resources,"ARRAY<STRUCT<displayName: STRING>>") as Resources 
# MAGIC from vw_sql_temp_string

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_json(Resources,"ARRAY<STRUCT<displayName: STRING, entitlements: ARRAY<STRUCT<value: STRING>>, groups: ARRAY<STRUCT<`$ref`: STRING, display: STRING, type: STRING, value: STRING>>, id: STRING, members: ARRAY<STRUCT<`$ref`: STRING, display: STRING, value: STRING>>, meta: STRUCT<resourceType: STRING>>>") as Resources 
# MAGIC from vw_sql_temp_string

# COMMAND ----------

# MAGIC %sql
# MAGIC select Resources.displayName,explode(Resources.members.display) from( 
# MAGIC select explode(from_json(Resources,"ARRAY<STRUCT<displayName: STRING, entitlements: ARRAY<STRUCT<value: STRING>>, groups: ARRAY<STRUCT<`$ref`: STRING, display: STRING, type: STRING, value: STRING>>, id: STRING, members: ARRAY<STRUCT<`$ref`: STRING, display: STRING, value: STRING>>, meta: STRUCT<resourceType: STRING>>>")) as Resources from vw_sql_temp_string
# MAGIC ) nivel1
