# Databricks notebook source
# DBTITLE 1,Defini variaveis notebook 1
dataini = '2023-01-01'
datafim = '2023-03-31'

# COMMAND ----------

# DBTITLE 1,Chama notebook 2 com notebook.run()
dbutils.notebook.run('/Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2',
                     -30, 
                     {"dataini": dataini, "datafim": datafim}
                    )

# COMMAND ----------

# DBTITLE 1,Utilizando %Run - Passando valores fixos funciona
# MAGIC %run /Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2 $dataini=2023-01-01 $datafim=2023-03-31

# COMMAND ----------

# DBTITLE 1,Passando varaveis não funciona
# MAGIC %run /Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2 $dataini=dataini $datafim=datafim

# COMMAND ----------

# DBTITLE 1,Definindo Widgets
dbutils.widgets.text("dataini", "2023-01-01")
dbutils.widgets.text("datafim", "2023-03-31")

# COMMAND ----------

# DBTITLE 1,utilizando widgets parece que funciona ne?
# MAGIC %run /Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2

# COMMAND ----------

# DBTITLE 1,Atualizando valores
dbutils.widgets.text("dataini", "2023-02-01")
dbutils.widgets.text("datafim", "2023-02-28")

# COMMAND ----------

# DBTITLE 1,Com widgets - Valores não atualizaram
# MAGIC %run /Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2

# COMMAND ----------

# DBTITLE 1,Limpando Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Criando tabela de parametros
# MAGIC %sql
# MAGIC drop table if exists tb_parameters;
# MAGIC create table if not exists tb_parameters (dataini date, datafim date);
# MAGIC insert into tb_parameters values('2023-01-01','2023-03-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tb_parameters

# COMMAND ----------

# DBTITLE 1,Agora sim!
# MAGIC %run /Users/reginaldo.silva@dataside.com.br/DemoRun/notebook3

# COMMAND ----------

# DBTITLE 1,Sessao do Spark
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

# DBTITLE 1,Contexto
# MAGIC %scala
# MAGIC spark.sparkContext

# COMMAND ----------

# DBTITLE 1,Testando com notebook.run()
dbutils.notebook.run('/Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2',
                     -30,
                     {"dataini": dataini, "datafim": datafim}
                    )

# COMMAND ----------

# DBTITLE 1,Testando sessao do Spark
# MAGIC %run /Users/reginaldo.silva@dataside.com.br/DemoRun/notebook2
