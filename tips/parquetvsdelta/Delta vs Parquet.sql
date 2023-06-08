-- Databricks notebook source
-- DBTITLE 1,Criando a tabela DEMO
-- MAGIC %py
-- MAGIC df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable("db_demo.PatientInfoDelta",path='abfss://xxx@xxx.dfs.core.windows.net/bronze/PatientInfoDelta')
-- MAGIC df.display()

-- COMMAND ----------

SET spark.databricks.delta.formatCheck.enabled=false

-- COMMAND ----------

-- DBTITLE 1,Lendo Parquets
select * from parquet.`abfss://xxx@xxx.dfs.core.windows.net/bronze/PatientInfoDelta/*.parquet`

-- COMMAND ----------

-- DBTITLE 1,Executando 1 Update na tabela Delta
-- Atualizando 1 registro
update db_demo.PatientInfoDelta set age = '33s' where patient_id = '1000000001';
select * from delta.`abfss://xxx@xxx.dfs.core.windows.net/bronze/PatientInfoDelta/`;

-- COMMAND ----------

-- DBTITLE 1,Lendo Parquets das tabelas Delta
select * from parquet.`abfss://xxx@xxx.dfs.core.windows.net/bronze/PatientInfoDelta/*.parquet`

-- COMMAND ----------

-- DBTITLE 1,Lendo parquets - spark.databricks.delta.formatCheck.enabled
SET spark.databricks.delta.formatCheck.enabled=true;
select * from parquet.`abfss://xxx@xxx.dfs.core.windows.net/bronze/PatientInfoDelta/*.parquet`
