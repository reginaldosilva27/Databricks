# Databricks notebook source
# DBTITLE 1,Sessao do Spark
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

# DBTITLE 1,Contexto do Spark
# MAGIC %scala
# MAGIC spark.sparkContext

# COMMAND ----------

print('ola mund0 - notebook2 aqui')

# COMMAND ----------

# DBTITLE 1,Mostrando variaveis do Notebook1
#Usando Argument
print(getArgument("dataini"))

#Usando Widgets
print(dbutils.widgets.get("datafim"))
