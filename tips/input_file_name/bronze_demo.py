# Databricks notebook source
# DBTITLE 1,Listando arquivos na camada Landing
# MAGIC %fs
# MAGIC ls abfss://datalake@storageunitycatalogdemo.dfs.core.windows.net/landing

# COMMAND ----------

# DBTITLE 1,Lendo arquivos JSON
# MAGIC %py
# MAGIC df = spark.read.option("multiLine", "True").json('abfss://datalake@storageunitycatalogdemo.dfs.core.windows.net/landing/*.json')
# MAGIC df.display()

# COMMAND ----------

# DBTITLE 1,Criando uma tabela Delta
# MAGIC %py
# MAGIC df.write.format('delta') \
# MAGIC .mode('overwrite') \
# MAGIC .saveAsTable("db_demo.person",path='abfss://datalake@storageunitycatalogdemo.dfs.core.windows.net/bronze/person')

# COMMAND ----------

# DBTITLE 1,Como saber de qual arquivo veio cada pessoa?
# MAGIC %sql
# MAGIC select name,* from db_demo.person

# COMMAND ----------

# DBTITLE 1,Usar na tabela Delta?
# MAGIC %sql
# MAGIC select input_file_name(),name,* from db_demo.person

# COMMAND ----------

# DBTITLE 1,Adicionando uma nova coluna nomeArquivo
# MAGIC %py
# MAGIC from  pyspark.sql.functions import input_file_name
# MAGIC df.withColumn("nomeArquivo",input_file_name()) \
# MAGIC .write.format('delta') \
# MAGIC .mode('overwrite') \
# MAGIC .option("overwriteSchema", True) \
# MAGIC .saveAsTable("db_demo.person",path='abfss://datalake@storageunitycatalogdemo.dfs.core.windows.net/bronze/person')

# COMMAND ----------

# DBTITLE 1,Agora sim!
# MAGIC %sql
# MAGIC select nomeArquivo,name,* from db_demo.person
