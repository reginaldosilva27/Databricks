-- Databricks notebook source
-- DBTITLE 1,SQL - Default desse notebook
-- MAGIC %sql
-- MAGIC -- Não preciso especificar, mas se quiser voce pode
-- MAGIC SELECT 'usando linguagem SQL'

-- COMMAND ----------

-- DBTITLE 1,Python
-- MAGIC %python
-- MAGIC var = "Opa, agora to no python!"
-- MAGIC print(var)

-- COMMAND ----------

-- DBTITLE 1,Shell script
-- MAGIC %sh
-- MAGIC ls -l

-- COMMAND ----------

-- DBTITLE 1,Scala
-- MAGIC %scala
-- MAGIC val var = "Vai de scala?"
-- MAGIC println(var)

-- COMMAND ----------

-- DBTITLE 1,R
-- MAGIC %r
-- MAGIC var <- "R é para os bruxos!"
-- MAGIC print(var)

-- COMMAND ----------

-- DBTITLE 1,Markdown
-- MAGIC %md
-- MAGIC ## Esse é o tema do post
-- MAGIC Vamos falar mais sobre Markdown

-- COMMAND ----------

-- DBTITLE 1,FS
-- MAGIC %fs
-- MAGIC ls /

-- COMMAND ----------

-- DBTITLE 1,run - chamando notebooks
-- MAGIC %run /maintenanceDeltalake
