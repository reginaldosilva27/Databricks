-- Databricks notebook source
-- DBTITLE 1,Criando ambiente
-- MAGIC %py
-- MAGIC df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
-- MAGIC df.count()

-- COMMAND ----------

-- DBTITLE 1,Exemplo dos dados
-- MAGIC %py
-- MAGIC df.display()

-- COMMAND ----------

-- DBTITLE 1,Gravando tabela particionada por pais
-- MAGIC %py
-- MAGIC df.write.format('parquet').mode('overwrite').partitionBy('Country').saveAsTable("db_demo.PatientInfoParquet_Country",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/table=PatientInfoParquet_Country')

-- COMMAND ----------

-- DBTITLE 1,Gravando tabela sem particionamento
-- MAGIC %py
-- MAGIC df.write.format('parquet').mode('overwrite').saveAsTable("db_demo.PatientInfoParquet_SemParticao",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/table=PatientInfoParquet_SemParticao')

-- COMMAND ----------

-- DBTITLE 1,Leitura com usando particionamento
select * from db_demo.PatientInfoParquet_Country where country = 'Canada'

-- COMMAND ----------

-- DBTITLE 1,Leitura sem particionamento
select * from db_demo.PatientInfoParquet_SemParticao where country = 'Canada'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable("db_demo.PatientInfoDeltaSemParticao",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/table=PatientInfoDeltaSemParticao')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df.write.format('delta').mode('overwrite').partitionBy('Country').saveAsTable("db_demo.PatientInfoDeltaCountry",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/table=PatientInfoDeltaCountry')

-- COMMAND ----------

select * from db_demo.PatientInfoDeltaSemParticao where country = 'Canada'

-- COMMAND ----------

select * from db_demo.PatientInfoDeltaCountry where country = 'Canada'

-- COMMAND ----------

OPTIMIZE db_demo.PatientInfoDelta ZORDER BY (country)

-- COMMAND ----------

-- DBTITLE 1,Exemplo de particionamento por várias colunas
-- MAGIC %py
-- MAGIC df.write.format("delta").mode("overwrite").partitionBy(
-- MAGIC     "country", "province", "city", "sex"
-- MAGIC ).saveAsTable(
-- MAGIC     "db_demo.PatientInfoDeltaParticionada",
-- MAGIC     path="abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/table=PatientInfoDeltaParticionada",
-- MAGIC )

-- COMMAND ----------

select * from parquet.`abfss://reginaldo@stdts360.dfs.core.windows.net/part-00000-acd72083-0f7c-4f3e-85d2-07fc39aa714c.c000.snappy.parquet`

-- COMMAND ----------

select * from (
select from_json(add.stats,'numRecords bigint').numRecords as numRecords, 
from_json(add.stats,'minValues struct<tickets_id:bigint>').minValues.tickets_id as minValues, 
from_json(add.stats,'maxValues struct<tickets_id:bigint>').maxValues.tickets_id as maxValues, 
add.path
from json.`abfss://xxxx@xxxx.dfs.core.windows.net/xxxx/logs2/_delta_log/00000000000000000002.json`
where add is not null
) tab where 22334863 between minValues and maxValues 
order by maxValues,minValues desc
