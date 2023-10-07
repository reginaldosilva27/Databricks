# Databricks notebook source
# DBTITLE 1,Instalando SDK
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Reiniciando Kernel
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Listando todos os clusters All Purpose
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host='adb-4013955633331914.14.azuredatabricks.net', token='xxxx')

for c in w.clusters.list():
  print(c.cluster_name)

# COMMAND ----------

# DBTITLE 1,Ligando Clusters
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host='adb-4013955633331914.14.azuredatabricks.net', token='xxxx')

for c in w.clusters.list():
  try:
      print('Ligando Cluster: ', c.cluster_name)
      w.clusters.start(cluster_id=c.cluster_id).result()
  except:
      print('Cluster já está ligado: ', c.cluster_name)

# COMMAND ----------

# DBTITLE 1,Listando todos os Jobs e quantidade de clusters e tasks
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host='adb-4013955633331914.14.azuredatabricks.net', token='xxxx')

job_list = w.jobs.list(expand_tasks=True)
for j in job_list:
  #print(j)
  print('job_id: ',j.job_id, ' - name:', j.settings.name, ' - job_clusters:', len(j.settings.job_clusters) if j.settings.job_clusters else 'None', ' - tasks:', len(j.settings.tasks), ' - tags:', j.settings.tags)

# COMMAND ----------

# DBTITLE 1,Listando todos os Notebooks e subpastas de usuário corrente
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host='adb-4013955633331914.14.azuredatabricks.net', token='xxxx')

names = []
for i in w.workspace.list(f'/Users/{w.current_user.me().user_name}', recursive=True):
    names.append(i.path)
    print(i.path)
assert len(names) > 0

# COMMAND ----------

# DBTITLE 1,Listando todos os Notebooks e subpastas do Workspace
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host='adb-4013955633331914.14.azuredatabricks.net', token='xxxx')

names = []
for i in w.workspace.list(f'/', recursive=True):
    names.append(i.path)
    print(i.path)
assert len(names) > 0

# COMMAND ----------

# DBTITLE 1,Listando Users do Workspace
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

w = WorkspaceClient(host='adb-4013955633331914.14.azuredatabricks.net', token='xxxx')

all_users = w.users.list(attributes="id,userName",
                         sort_by="userName",
                         sort_order=iam.ListSortOrder.DESCENDING)

for u in all_users:
    print(u.user_name)
