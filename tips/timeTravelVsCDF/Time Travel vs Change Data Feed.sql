-- Databricks notebook source
-- DBTITLE 1,Criando tabela demo
-- MAGIC %py
-- MAGIC df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable("db_demo.PatientInfoDelta",path='abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/PatientInfoDelta')
-- MAGIC df.display()

-- COMMAND ----------

-- DBTITLE 1,Time travel exemplo
-- Atualizando 1 registro
update db_demo.PatientInfoDelta set age = '33s' where patient_id = '1000000001'

-- COMMAND ----------

-- DBTITLE 1,Visualizando o histórico de alterações na tabela
describe history db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Viajando no tempo usando VERSION AS OF
select * from db_demo.PatientInfoDelta VERSION AS OF 0 where patient_id = '1000000001'

-- COMMAND ----------

-- DBTITLE 1,Viajando no tempo usando TIMESTAMP AS OF
select 'OLD', * from db_demo.PatientInfoDelta timestamp AS OF '2023-06-03T14:19:07.000+0000' 
where patient_id = '1000000001' union all 
select 'NEW', * from db_demo.PatientInfoDelta where patient_id = '1000000001'

-- COMMAND ----------

-- DBTITLE 1,DELETE Sem Where - E agora quem poderá nos defender?
delete from db_demo.PatientInfoDelta;
select * from db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Historico de alterações
describe history db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Restaurando a tabela com historico do TIME TRAVEL
RESTORE db_demo.PatientInfoDelta VERSION AS OF 1;
select * from db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Habilitando o Change Data Feed
Alter table db_demo.PatientInfoDelta SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- DBTITLE 1,Criar nossa tabela Silver para simular o CDF na prática
-- Essa silver só terá dados de pacientes infectados por outros pacientes filtrando pelo infected_by
Create or Replace table db_demo.SilverPatientInfectedBy
as
select patient_id,sex,age,country,province,city,infection_case,infected_by from db_demo.PatientInfoDelta where infected_by is not null;

select * from db_demo.SilverPatientInfectedBy;

-- COMMAND ----------

-- DBTITLE 1,Gerando algumas modificações
-- Atualizando 2 registro, deletando 1 registro e inserindo 1 registro
-- Note que estou aplicando 2 updates no mesmo registro 1000000003
update db_demo.PatientInfoDelta set age = '70s' where patient_id = '1000000003';
update db_demo.PatientInfoDelta set sex = 'female' where patient_id = '1000000003';
delete from db_demo.PatientInfoDelta where patient_id = '1000000005';
insert into db_demo.PatientInfoDelta values('1000003211','male','31s','Brazil','Sao Paulo','Boituva','Dataholic','1500000033',12,current_date(),current_date(),current_date(),null,'released');

-- COMMAND ----------

-- DBTITLE 1,Visualizando as versões
describe history db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Usando table_changes() para navegar nas versões
-- Pegando a partir da versão 4 tudo que aconteceu
SELECT _change_type,_commit_version,_commit_timestamp,* FROM table_changes('db_demo.`PatientInfoDelta`', 4)

-- COMMAND ----------

-- DBTITLE 1,Criando uma View temporaria para pegar somente a ultima versão de cada registro
-- Para Updates pegamos somente o update_postimage que são os dados novos
-- Estamos aplicando a função ROW_NUMBER pela chave da tabela (patient_id) ordenando pelo _commit_version
-- Note que o rnb filtramos apenas o 1, então se o paciente tiver 2 Updates será aplicado o mais recente
-- Estou passando a versão fixa no table_changes, mas pode ser dinamico
CREATE OR REPLACE TEMPORARY VIEW vwPatientInfectedBy as
SELECT * 
    FROM 
         (SELECT *, row_number() over (partition by patient_id order by _commit_version desc) as rnb
          FROM table_changes('db_demo.`PatientInfoDelta`', 4) where _change_type !='update_preimage' and infected_by is not null)
    WHERE rnb=1;

select _change_type,_commit_version,_commit_timestamp,rnb,* from vwPatientInfectedBy;

-- COMMAND ----------

-- DBTITLE 1,Visualizando alterações antes
select * from db_demo.SilverPatientInfectedBy where patient_id in('1000000003','1000000005','1000003211');

-- COMMAND ----------

-- DBTITLE 1,Aplicando as alterações na nossa tabela Silver
MERGE INTO db_demo.SilverPatientInfectedBy as t 
USING vwPatientInfectedBy as s
ON s.patient_id = t.patient_id
WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
WHEN MATCHED AND s._change_type = 'update_postimage' THEN UPDATE SET * 
WHEN NOT MATCHED AND _change_type != 'delete' THEN INSERT  *

-- COMMAND ----------

-- DBTITLE 1,Visualizando alterações depois
select * from db_demo.SilverPatientInfectedBy where patient_id in('1000000003','1000000005','1000003211');

-- COMMAND ----------

-- DBTITLE 1,Sem CDC daria pra fazer?
-- Somente para INSERT e UPDATE, Delete não é replicavel, a não ser que voce compare as tabelas inteiras, que na maioria dos casos não é viável, pois são cargas incrementais
-- Nome a quantidade de escrita, praticamente a tabela Silver inteira foi reescrita
MERGE INTO db_demo.SilverPatientInfectedBy as t 
USING  db_demo.PatientInfoDelta as s
ON s.patient_id = t.patient_id and s.infected_by is not null
WHEN MATCHED
  THEN UPDATE SET *
WHEN NOT MATCHED and s.infected_by is not null
  THEN INSERT * 

-- COMMAND ----------

-- DBTITLE 1,Restaurar um DELETE\UPDATE sem WHERE com CDF?
UPDATE db_demo.PatientInfoDelta set age = '10s';
select * from db_demo.PatientInfoDelta

-- COMMAND ----------

describe history db_demo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Olhando somente a versão 9
SELECT _change_type,_commit_version,_commit_timestamp,* FROM table_changes('db_demo.`PatientInfoDelta`', 9,9) 
where _change_type = 'update_preimage'

-- COMMAND ----------

-- DBTITLE 1,Voltando um Update sem WHERE com CDF
-- Voltando todos os UPDATES da versão 9
MERGE INTO db_demo.PatientInfoDelta as t 
USING  (SELECT row_number() over (partition by patient_id order by _commit_version desc) as rnb,* 
FROM table_changes('db_demo.`PatientInfoDelta`', 9,9)  where _change_type = 'update_preimage') as s
ON s.patient_id = t.patient_id and _change_type = 'update_preimage' and rnb = 1
WHEN MATCHED
  THEN UPDATE SET *

-- COMMAND ----------

-- DBTITLE 1,É, funciona também
select * from db_demo.PatientInfoDelta;
