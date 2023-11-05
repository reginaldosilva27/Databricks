-- Databricks notebook source
SHOW CATALOGS

-- COMMAND ----------

-- Databases = Schemas
SHOW SCHEMAS FROM DEV

-- COMMAND ----------

SHOW TABLES FROM DEV.db_demo

-- COMMAND ----------

USE CATALOG DEV;
USE SCHEMA db_demo;
SHOW TABLE EXTENDED LIKE 'tb*';

-- COMMAND ----------

DROP TABLE testeuc2

-- COMMAND ----------

SHOW TABLES DROPPED IN db_demo

-- COMMAND ----------

UNDROP table DEV.db_demo.testeuc2

-- COMMAND ----------

ALTER TABLE DEV.db_demo.tbordersliquid SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '1 days');

-- COMMAND ----------

SHOW TBLPROPERTIES DEV.db_demo.tbordersliquid

-- COMMAND ----------

SHOW COLUMNS FROM DEV.db_demo.tbordersliquid

-- COMMAND ----------

-- MAGIC %py
-- MAGIC listcolunas = ''
-- MAGIC list = spark.sql('SHOW COLUMNS FROM DEV.db_demo.tbordersliquid').collect()
-- MAGIC
-- MAGIC print(listcolunas)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC listcolunas = ','.join(str(col.col_name) for col in spark.sql('SHOW COLUMNS FROM DEV.db_demo.tbordersliquid').collect())
-- MAGIC print(listcolunas)

-- COMMAND ----------

SHOW CREATE TABLE DEV.db_demo.tbordersliquid

-- COMMAND ----------

SHOW PARTITIONS DEV.db_demo.tborderspartition

-- COMMAND ----------

SHOW USERS

-- COMMAND ----------

SHOW USERS LIKE '*dataside*'

-- COMMAND ----------

SHOW GROUPS

-- COMMAND ----------

SHOW GROUPS WITH USER `reginaldo.silva@dataside.com.br`;

-- COMMAND ----------

SHOW GROUPS WITH GROUP `read_write_prod`;

-- COMMAND ----------

USE CATALOG DEV

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC from datetime import datetime
-- MAGIC countTBOk = 1
-- MAGIC countError = 0
-- MAGIC countTotal = 1
-- MAGIC for db in spark.sql("show databases").collect():  
-- MAGIC   print('>>>>>>>> iniciando DB: ',db.databaseName)
-- MAGIC   for tb in spark.sql(f"show tables from {db.databaseName}").collect():
-- MAGIC     try:
-- MAGIC       countTotal = countTotal + 1
-- MAGIC       print(countTotal,' - ',db.databaseName,'.',tb.tableName)
-- MAGIC       spark.sql(f"select * from {db.databaseName}.{tb.tableName} limit 1")
-- MAGIC       countTBOk = countTBOk + 1
-- MAGIC     except Exception as error:
-- MAGIC       print("#######error ocurred on: ", db.databaseName,'.',tb.tableName, error)
-- MAGIC       countError = countError + 1
-- MAGIC       print ('------Quantidade de erros:', countError)
-- MAGIC
-- MAGIC print('Tabelas OK: ', countTBOk)
-- MAGIC print('Tabelas com Erro: ', countError)
-- MAGIC print('Total tabelas: ', countTotal)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC from datetime import datetime
-- MAGIC countTotal = 0
-- MAGIC for db in spark.sql("show databases").collect():  
-- MAGIC   print('>>>>>>>> iniciando DB: ',db.databaseName)
-- MAGIC   for tb in spark.sql(f"show tables from {db.databaseName}").collect():
-- MAGIC     try:
-- MAGIC       countTotal = countTotal + 1
-- MAGIC       print(countTotal,' - ',str(db.databaseName).replace(' ',''),'.',str(tb.tableName).replace(' ',''))
-- MAGIC       listcolunas = ','.join(str(col.col_name) for col in spark.sql(f"""SHOW COLUMNS FROM {db.databaseName.replace(' ','')}.{tb.tableName} """).collect())
-- MAGIC       print('->>> TableName: ',db.databaseName,'.',tb.tableName, ' ->>> List Cols: ',listcolunas)
-- MAGIC     except Exception as error:
-- MAGIC       print("#######error ocurred on: ", db.databaseName,'.',tb.tableName, error)
