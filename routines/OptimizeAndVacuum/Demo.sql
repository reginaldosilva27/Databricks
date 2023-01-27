-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.fs.rm('/mnt/raw/database=covid/table=PatientInfoDelta',True)

-- COMMAND ----------

-- DBTITLE 1,Preparando o ambiente
-- MAGIC %py
-- MAGIC #Ambiente lendo CSV de exemplos e salvando como tabela DELTA
-- MAGIC df = spark.read.option("header", "True").format('csv').load('/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv')
-- MAGIC #Salve onde quiser, estou usando um Mount para facilitar
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable("db_festivaldemo.PatientInfoDelta",path='/mnt/raw/database=covid/table=PatientInfoDelta')
-- MAGIC count = 0
-- MAGIC #Alguns Updates para gerar alguns logs e tudo pronto
-- MAGIC while count < 12:
-- MAGIC     spark.sql(f"update db_festivaldemo.PatientInfoDelta set age={count} where patient_id = 1000000001")
-- MAGIC     print(count)
-- MAGIC     count=count+1

-- COMMAND ----------

-- DBTITLE 1,Instanciar o notebook utilizando o RUN
-- MAGIC %run /Users/reginaldo.silva@dataside.com.br/OptimizeAndVacuum

-- COMMAND ----------

-- DBTITLE 1,Detalhes da tabela
-- Olhe o campo numFiles
describe detail db_festivaldemo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Quantidade de arquivos no Storage
-- MAGIC %py
-- MAGIC #note que temos 13 arquivos de dados, contudo no numFiles temos apenas 1, ou seja, esses 12 sao historico e podem ser limpos se na forem ser usados para Time Travel
-- MAGIC len(dbutils.fs.ls('dbfs:/mnt/raw/database=covid/table=PatientInfoDelta')) 

-- COMMAND ----------

-- DBTITLE 1,Historico
-- Todas as alterações que fizemos com o Loop
describe history db_festivaldemo.PatientInfoDelta

-- COMMAND ----------

-- DBTITLE 1,Usando a função: Chamando com Debug habilitado
-- MAGIC %py
-- MAGIC #Chamando a função instanciada no notebook
-- MAGIC #Usando 2 colunas no ZORDER, apenas para exemplo
-- MAGIC maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='sex,patient_id', vacuumRetention=144, vacuum=True, optimize=True, debug=True)

-- COMMAND ----------

-- DBTITLE 1,Usando a função: Executando
-- MAGIC %py
-- MAGIC #Chamando a função instanciada no notebook
-- MAGIC #Usando 0 horas apenas para exemplo, o recomendado é 7 dias
-- MAGIC maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='sex,patient_id', vacuumRetention=0, vacuum=True, optimize=True, debug=False)

-- COMMAND ----------

-- DBTITLE 1,Rodando sem HIVE
-- MAGIC %py
-- MAGIC #Execuando passando o caminho direto no Lake, defina o schema como delta e coloca o caminho entre `caminho`
-- MAGIC maintenanceDeltalake(nomeSchema='delta', nomeTabela='`/mnt/raw/database=covid/table=PatientInfoDelta`', colunasZorder='sex,patient_id', vacuumRetention=144, vacuum=True, optimize=True, debug=False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #recontagem
-- MAGIC len(dbutils.fs.ls('dbfs:/mnt/raw/database=covid/table=PatientInfoDelta')) 
