-- Databricks notebook source
-- DBTITLE 1,Cria tabela básica sem nenhuma feature nova
create table tb_teste (campo1 int);
describe extended tb_teste;

-- COMMAND ----------

-- DBTITLE 1,Realizando upgrade para utilizar o CDC
-- Upgrades the reader protocol version to 1 and the writer protocol version to 4.
ALTER TABLE tb_teste SET TBLPROPERTIES('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '4');
describe extended tb_teste;

-- COMMAND ----------

-- DBTITLE 1,Criando uma tabela com CDC habilitado
create table tb_teste2 (campo1 int) TBLPROPERTIES (delta.enableChangeDataFeed = true);
describe extended tb_teste2;

-- COMMAND ----------

-- DBTITLE 1,Criando uma tabela com a ultima versão para usar Deletion Vector
drop table tb_teste3;
create table tb_teste3 (campo1 int) TBLPROPERTIES('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7');
describe extended tb_teste3;

-- COMMAND ----------

-- DBTITLE 1,Habilitando Deletion Vector
alter table tb_teste3 SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
describe extended tb_teste3;

-- COMMAND ----------

-- DBTITLE 1,Tentando usar uma feature não suportada pelo Databricks Runtime
alter table tb_teste3 SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')

-- COMMAND ----------

-- DBTITLE 1,Downgrade
ALTER TABLE tb_teste3 SET TBLPROPERTIES('delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '4')

-- COMMAND ----------

create table tb_teste4 (campo1 int) TBLPROPERTIES (delta.enableChangeDataFeed = true);
describe extended tb_teste4;

-- COMMAND ----------

-- DBTITLE 1,Tentando ler a tabela com Runtime 11.3
select * from tb_teste3

-- COMMAND ----------

-- DBTITLE 1,Habilitando timestampNtz
create table tb_teste5 (campo1 int) TBLPROPERTIES (delta.feature.timestampNtz = 'supported');
describe extended tb_teste5;

-- COMMAND ----------

create table tb_teste6 (campo1 int) TBLPROPERTIES (delta.feature.enableDeletionVectors = 'supported');
describe extended tb_teste6;

-- COMMAND ----------

-- DBTITLE 1,Resumo do Table Features
CREATE TABLE db_demo.teste7 (
  patient_id STRING)
USING delta
LOCATION 'abfss://reginaldo@stdts360.dfs.core.windows.net/bronze/teste7'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
