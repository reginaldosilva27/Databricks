# Databricks notebook source
# DBTITLE 1,Cria uma nova tabela
df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
df.write.format('delta').mode('overwrite').saveAsTable("db_demo.PatientInfoDelta",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDelta')
df.count()

# COMMAND ----------

# DBTITLE 1,Ver detalhes da tabela
# MAGIC %sql
# MAGIC describe extended db_demo.PatientInfoDelta

# COMMAND ----------

# DBTITLE 1,Delete sem Deletion Vector
# MAGIC %sql
# MAGIC delete from db_demo.PatientInfoDelta where patient_id = 1000000002

# COMMAND ----------

# DBTITLE 1,Habilitar Deletion Vector - Irá realizar Upgrade do protocolo Delta
# MAGIC %sql
# MAGIC ALTER TABLE db_demo.PatientInfoDelta SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

# COMMAND ----------

# DBTITLE 1,Delete com Deletion Vector
# MAGIC %sql
# MAGIC delete from db_demo.PatientInfoDelta where patient_id = 1000000001

# COMMAND ----------

# DBTITLE 1,COUNT para validar
# MAGIC %sql
# MAGIC select count(*) from db_demo.PatientInfoDelta

# COMMAND ----------

# DBTITLE 1,Update com Deletion Vector? Somente com Photon
# MAGIC %sql
# MAGIC update db_demo.PatientInfoDelta set sex = 'male' where patient_id = '1000000033'

# COMMAND ----------

# DBTITLE 1,Limpando versões e deletion vectors
# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM db_demo.PatientInfoDelta  RETAIN 0 HOURS 

# COMMAND ----------

# DBTITLE 1,Deletes com Deletion Vector - Testando performance
id = 1000000001
while 1 == 1:
    spark.sql(f"delete from db_demo.PatientInfoDelta where patient_id = {id}")
    print(id)
    id=id+1

# COMMAND ----------

df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
df.write.format('delta').mode('overwrite').saveAsTable("db_demo.PatientInfoDeltaSemDeletion",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDeltaSemDeletion')

# COMMAND ----------

# DBTITLE 1,Deletes SEM Deletion Vector - Testando performance
id = 1000000001
while 1 == 1:
    spark.sql(f"delete from db_demo.PatientInfoDeltaSemDeletion where patient_id = {id}")
    print(id)
    id=id+1
