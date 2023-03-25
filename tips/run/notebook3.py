# Databricks notebook source
print('ola mund0 - notebook3 aqui')

# COMMAND ----------

# DBTITLE 1,Definindo váriaveis com retorno de uma query
dataini = spark.sql("select dataini from tb_parameters")[0][0]
datafim = spark.sql("select datafim from tb_parameters")[0][0]

# COMMAND ----------

# DBTITLE 1,Mostrando variaveis do Notebook1
#Usando Argument
print(getArgument("dataini"))

#Usando Widgets
print(dbutils.widgets.get("datafim"))
