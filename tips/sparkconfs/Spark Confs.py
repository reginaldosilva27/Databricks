# Databricks notebook source
# DBTITLE 1,Mostrar todas as configurações disponíveis
df = spark.sparkContext.getConf().getAll()
i=1
for d in df:
  print(str(i),' - ',d)
  i = i+1

# COMMAND ----------

# DBTITLE 1,Mostrar todas as ClusterTags
df = spark.sparkContext.getConf().getAll()
i=1
for d in df:
  if 'clusterUsageTags' in d[0]:
    print(str(i),' - ',d)
    i=i+1

# COMMAND ----------

# DBTITLE 1,Cluster tags mais comuns
print(
' | Description                                                      | Value                            | Description                                 |\n',
' ----------------------------------------------------------------------------------------------------------------------------------------------------\n',
'| spark.databricks.clusterUsageTags.cloudProvider               | ',spark.conf.get('spark.databricks.clusterUsageTags.cloudProvider'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.cloudProvider'))) * ' ','| Cloud que esta operando o Databricks', '|\n',
'| spark.databricks.clusterUsageTags.azureSubscriptionId         | ',spark.conf.get('spark.databricks.clusterUsageTags.azureSubscriptionId'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.azureSubscriptionId'))) * ' ','| ID da assinatura do Azure', '|\n',
'| spark.databricks.clusterUsageTags.managedResourceGroup        | ',spark.conf.get('spark.databricks.clusterUsageTags.managedResourceGroup'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.managedResourceGroup'))) * ' ','| Grupo de recursos no Azure que é gerenciado pelo Databricks', '|\n',
'| spark.databricks.clusterUsageTags.clusterId                   | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterId'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterId'))) * ' ','| ID do cluster', '|\n',
'| spark.databricks.clusterUsageTags.region                      | ',spark.conf.get('spark.databricks.clusterUsageTags.region'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.region'))) * ' ','| Região que hospeda os Clusters', '|\n',
'| spark.databricks.clusterUsageTags.workerEnvironmentId         | ',spark.conf.get('spark.databricks.clusterUsageTags.workerEnvironmentId'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.workerEnvironmentId'))) * ' ','| ID do Worksapce', '|\n',
'| spark.databricks.clusterUsageTags.region                      | ',spark.conf.get('spark.databricks.clusterUsageTags.region'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.region'))) * ' ','| Região que hospeda os Clusters', '|\n',
'| spark.databricks.clusterUsageTags.clusterLogDestination       | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterLogDestination'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterLogDestination'))) * ' ','| Caminho onde os logs serão entregues', '|\n',
'| spark.databricks.clusterUsageTags.isSingleUserCluster         | ',spark.conf.get('spark.databricks.clusterUsageTags.isSingleUserCluster'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.isSingleUserCluster'))) * ' ','| É um cluster de usuario unico?', '|\n',
'| spark.databricks.clusterUsageTags.clusterName                 | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterName'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterName'))) * ' ','| Nome do Cluster', '|\n',
'| spark.databricks.clusterUsageTags.clusterScalingType          | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterScalingType'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterScalingType'))) * ' ','| Tem auto scale?', '|\n',
'| spark.databricks.clusterUsageTags.clusterNodeType             | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterNodeType'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterNodeType'))) * ' ','| Familia da maquina para os nodes', '|\n',
'| spark.databricks.clusterUsageTags.driverNodeType              | ',spark.conf.get('spark.databricks.clusterUsageTags.driverNodeType'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.driverNodeType'))) * ' ','| Familia da maquina para o Driver', '|\n',
'| spark.databricks.clusterUsageTags.clusterWorkers              | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterWorkers'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterWorkers'))) * ' ','| Quantidade de workers Online', '|\n',
'| spark.databricks.clusterUsageTags.effectiveSparkVersion       | ',spark.conf.get('spark.databricks.clusterUsageTags.effectiveSparkVersion'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.effectiveSparkVersion'))) * ' ','| Versão do Spark operando no Cluster', '|\n',
'| spark.databricks.clusterUsageTags.clusterSku                  | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterSku'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterSku'))) * ' ','| Tipo do Cluster', '|\n',
'| spark.databricks.clusterUsageTags.clusterAvailability         | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterAvailability'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterAvailability'))) * ' ','| Tipo de VMs em uso, SPOT ou On Demand', '|\n',
'| spark.databricks.clusterUsageTags.enableElasticDisk           | ',spark.conf.get('spark.databricks.clusterUsageTags.enableElasticDisk'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.enableElasticDisk'))) * ' ','| Possui discos elasticos para escalar?', '|\n',
'| spark.databricks.clusterUsageTags.autoTerminationMinutes      | ',spark.conf.get('spark.databricks.clusterUsageTags.autoTerminationMinutes'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.autoTerminationMinutes'))) * ' ','| Desligar cluster automaticamente após X minutos', '|\n',
'| spark.databricks.clusterUsageTags.runtimeEngine               | ',spark.conf.get('spark.databricks.clusterUsageTags.runtimeEngine'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.runtimeEngine'))) * ' ','| Tipo da Engine em execução', '|\n',
'| spark.databricks.clusterUsageTags.clusterLastActivityTime     | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterLastActivityTime'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterLastActivityTime'))) * ' ','| Data da ultima atividade executada no cluster', '|\n',
'| spark.databricks.clusterUsageTags.enableCredentialPassthrough | ',spark.conf.get('spark.databricks.clusterUsageTags.enableCredentialPassthrough'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.enableCredentialPassthrough'))) * ' ','| Cluster com Passthrough habilitado?', '|\n',
'| spark.databricks.clusterUsageTags.instanceWorkerEnvNetworkType| ',spark.conf.get('spark.databricks.clusterUsageTags.instanceWorkerEnvNetworkType'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.instanceWorkerEnvNetworkType'))) * ' ','|(vnet-injection) ou Default?', '|\n',
'| spark.databricks.clusterUsageTags.enableLocalDiskEncryption   | ',spark.conf.get('spark.databricks.clusterUsageTags.enableLocalDiskEncryption'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.enableLocalDiskEncryption'))) * ' ','| Criptografica local?', '|\n',
'| park.databricks.clusterUsageTags.clusterOwnerOrgId            | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId'))) * ' ','| ID da organização, faz parte da URL do Workspace', '|\n',
'| spark.databricks.clusterUsageTags.clusterPythonVersion        | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterPythonVersion'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterPythonVersion'))) * ' ','| Versão do Python rodando no Cluster', '|\n',
'| spark.databricks.clusterUsageTags.enableDfAcls                | ',spark.conf.get('spark.databricks.clusterUsageTags.enableDfAcls'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.enableDfAcls'))) * ' ','| Possui ACL habilitado?', '|\n',
'| spark.databricks.clusterUsageTags.instanceWorkerEnvId         | ',spark.conf.get('spark.databricks.clusterUsageTags.instanceWorkerEnvId'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.instanceWorkerEnvId'))) * ' ','| ID da Instancia', '|\n',
'| spark.databricks.clusterUsageTags.clusterUnityCatalogMode     | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterUnityCatalogMode'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterUnityCatalogMode'))) * ' ','| Utiliza Unity Catalog?', '|\n',
'| spark.databricks.clusterUsageTags.enableSqlAclsOnly           | ',spark.conf.get('spark.databricks.clusterUsageTags.enableSqlAclsOnly'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.enableSqlAclsOnly'))) * ' ','| ACL com SQL habilitado?', '|\n',
'| spark.databricks.clusterUsageTags.clusterPinned               | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterPinned'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterPinned'))) * ' ','| Cluster esta pinado?', '|\n',
'| spark.databricks.clusterUsageTags.privateLinkEnabled          | ',spark.conf.get('spark.databricks.clusterUsageTags.privateLinkEnabled'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.privateLinkEnabled'))) * ' ','| Possui Private Link habilitado?', '|\n',
'| spark.databricks.clusterUsageTags.clusterCreator              | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterCreator'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterCreator'))) * ' ','| Cluster criador por', '|\n',
'| spark.databricks.clusterUsageTags.clusterNumCustomTags        | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterNumCustomTags'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterNumCustomTags'))) * ' ','| Quantidade de tags customizadas', '|\n',
'| spark.databricks.clusterUsageTags.clusterAllTags              | ',spark.conf.get('spark.databricks.clusterUsageTags.clusterAllTags'),(40-len(spark.conf.get('spark.databricks.clusterUsageTags.clusterAllTags'))) * ' ','| Quantidade de tags customizadas', '|\n',
' ----------------------------------------------------------------------------------------------------------------------------------------------------\n',
'Links Reference:\n',
'https://spark.apache.org/docs/latest/configuration.html \n',
'https://books.japila.pl/delta-lake-internals/'
)

# COMMAND ----------

# DBTITLE 1,Spark confs comuns
print(
' | Description                                                      | Value                            | Description                                 |\n',
' ----------------------------------------------------------------------------------------------------------------------------------------------------\n',
'| spark.databricks.cloudProvider                              | ',spark.conf.get('spark.databricks.cloudProvider'),(40-len(spark.conf.get('spark.databricks.cloudProvider'))) * ' ','| Cloud que esta operando o Databricks', '|\n',
'| spark.databricks.workspaceUrl                               | ',spark.conf.get('spark.databricks.workspaceUrl'),(40-len(spark.conf.get('spark.databricks.workspaceUrl'))) * ' ','| URL para acessar o Worksapce', '|\n',
'| spark.app.startTime                                         | ',spark.conf.get('spark.app.startTime'),(40-len(spark.conf.get('spark.app.startTime'))) * ' ','| Hora de inicio da aplicação Spark', '|\n',
'| spark.app.name                                              | ',spark.conf.get('spark.app.name'),(40-len(spark.conf.get('spark.app.name'))) * ' ','| Nome da aplicação Spark', '|\n',
'| spark.app.id                                                | ',spark.conf.get('spark.app.id'),(40-len(spark.conf.get('spark.app.id'))) * ' ','| Id da aplicação criada pelo Spark   ', '|\n',
'| spark.databricks.clusterSource                              | ',spark.conf.get('spark.databricks.clusterSource'),(40-len(spark.conf.get('spark.databricks.clusterSource'))) * ' ','| Cluster criado via UI, JOB or API', '|\n',
'| spark.driver.maxResultSize                                  | ',spark.conf.get('spark.driver.maxResultSize'),(40-len(spark.conf.get('spark.driver.maxResultSize'))) * ' ','| Tamanho máximo do retorno de uma ação, exemplo Collect(), caso contrario será abortado para evitar problemas no Driver', '|\n',
'| spark.sql.sources.default                                   | ',spark.conf.get('spark.sql.sources.default'),(40-len(spark.conf.get('spark.sql.sources.default'))) * ' ','| Padrão de fonte utilizada, no Spark puro o default é Parquet', '|\n',
'| spark.databricks.delta.multiClusterWrites.enabled           | ',spark.conf.get('spark.databricks.delta.multiClusterWrites.enabled'),(40-len(spark.conf.get('spark.databricks.delta.multiClusterWrites.enabled'))) * ' ','| Permite escrita por mais de um cluster paralelo', '|\n',
'| spark.databricks.workerNodeTypeId                           | ',spark.conf.get('spark.databricks.workerNodeTypeId'),(40-len(spark.conf.get('spark.databricks.workerNodeTypeId'))) * ' ','| Familia da VM utilizada nos Workers do Cluster', '|\n',
'| spark.driver.host                                           | ',spark.conf.get('spark.driver.host'),(40-len(spark.conf.get('spark.driver.host'))) * ' ','| IP da VM do Driver', '|\n',
'| spark.master                                                | ',spark.conf.get('spark.master'),(40-len(spark.conf.get('spark.master'))) * ' ','| Gerenciador do Cluster', '|\n',
'| spark.databricks.driverNodeTypeId                           | ',spark.conf.get('spark.databricks.driverNodeTypeId'),(40-len(spark.conf.get('spark.databricks.driverNodeTypeId'))) * ' ','| Familia da VM utilizada no Driver do Cluster', '|\n',
'| spark.executor.memory                                       | ',spark.conf.get('spark.executor.memory'),(40-len(spark.conf.get('spark.executor.memory'))) * ' ','| Quantidade de memoria RAM nos Workers', '|\n',
'| spark.sql.hive.metastore.version                            | ',spark.conf.get('spark.sql.hive.metastore.version'),(40-len(spark.conf.get('spark.sql.hive.metastore.version'))) * ' ','| Versão do Metastore', '|\n',
'| spark.databricks.automl.serviceEnabled                      | ',spark.conf.get('spark.databricks.automl.serviceEnabled'),(40-len(spark.conf.get('spark.databricks.automl.serviceEnabled'))) * ' ','| Validar se o serviço de ML esta habilitado', '|\n',
' ----------------------------------------------------------------------------------------------------------------------------------------------------\n',
'Links Reference:\n',
'https://spark.apache.org/docs/latest/configuration.html \n',
'https://books.japila.pl/delta-lake-internals/'
)

# COMMAND ----------

# DBTITLE 1,Environment Variables
import os
i=1
for var in os.environ.items():
  print(str(i),' - ',var)
  i = i+1
