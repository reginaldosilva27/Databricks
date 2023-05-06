-- Databricks notebook source
-- DBTITLE 1,Cria tabela de teste e COUNT de linhas
-- MAGIC %py
-- MAGIC df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable("db_demo.PatientInfoDelta",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDelta')
-- MAGIC df.count()

-- COMMAND ----------

-- DBTITLE 1,Visualizar os dados
select * from db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Ver plano de execução
explain extended select count(*) from db_demo.monitoramento

-- COMMAND ----------

-- DBTITLE 1,Insere novo registro para gerar uma nova versão
insert into db_demo.PatientInfoDelta values('1000003211','male','31s','Brazil','Sao Paulo','Boituva','Dataholic',null,12,current_date(),current_date(),current_date(),null,'released')

-- COMMAND ----------

-- DBTITLE 1,Deleta 11 registros para gerar uma nova versão
delete from db_demo.PatientInfoDelta where patient_id between '1000000001' and '1000000011'

-- COMMAND ----------

-- DBTITLE 1,Visualizar tamanho da tabela
describe detail db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Visualizar versões da tabela
describe history db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Count de linha atual
select count(*) from db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,COUNT usando o Delta Log
select
  sum(from_json(
    add.stats,'numRecords DOUBLE'
  ).numRecords) as numRecordsAdd
from
  json.`abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDelta/_delta_log/0000000000000000*.json`
  where add is not null
  and add.path NOT IN (
    select remove.path from json.`abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDelta/_delta_log/0000000000000000*.json`
    where remove is not null
  )

-- COMMAND ----------

-- DBTITLE 1,Visualizando os metados do _delta_log
select
  from_json(
    add.stats,'numRecords DOUBLE'
  ).numRecords as numRecordsAdd,
*
from
  json.`abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDelta/_delta_log/0000000000000000*.json`
