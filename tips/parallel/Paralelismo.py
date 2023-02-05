# Databricks notebook source
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor    

# COMMAND ----------

# DBTITLE 1,Serial Way
# Função que recebe um numero e printa ele na tela junto com a data e hora
def printNumber(number):
        try:
            print(f"{number} - {datetime.today()}")
            time.sleep(1)
        except:
            print(number + ' - ' + str(datetime.today()))

# Gerando uma lista de numeros e passando cada um para a função
numbers = range(1,11)
[printNumber(i) for i in numbers]

# COMMAND ----------

# DBTITLE 1,Parallel Way
# Essa é a mesma função de printar o numero usada no serial
def printNumber(number):
        try:
            print(f"{number} - {datetime.today()}")
            time.sleep(1)
        except:
            print(number + ' - ' + str(datetime.today()))

# Criamos uma função que irá receber uma lista de numeros e printar ele de forma paralela
# Note que especificamos tambem a quantidade maxima de paralelismo que pode ser usada
def parallelInt(numbers, numInParallel):
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(printNumber, number) for number in numbers]

# Definindo a lista de numeros e quantidade de threads em paralelo
numbers = range(1,11)
parallelThreads = 10
print(numbers)
result = parallelInt(numbers,parallelThreads)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history db_festivaldemo.PatientInfoDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gerando Delete de exemplo
# MAGIC delete from db_festivaldemo.PatientInfoDelta where patient_id = 1000000002

# COMMAND ----------

# Executando um COUNT(*) em cada versão da tabela
listVersions = spark.sql("describe history db_festivaldemo.PatientInfoDelta").collect()
for row in listVersions:
  print(f'Version -> {row.version} - Count: {spark.sql(f"select count(*) as qtd from db_festivaldemo.PatientInfoDelta VERSION AS OF {row.version}").collect()[0][0]} - {datetime.today()}')

# COMMAND ----------

# Função para executar um count em cada versão da tabela
def getversion(version):
        try:
            print(f'Version -> {version} - Count: {spark.sql(f"select count(*) as qtd from db_festivaldemo.PatientInfoDelta VERSION AS OF {version}").collect()[0][0]} - {datetime.today()}')
        except:
            print(version + ' - ' + str(datetime.today()))
            
def parallelInt2(numbers, numInParallel):
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(getversion, item.version) for item in listVersions]
      
listVersions = spark.sql("describe history db_festivaldemo.PatientInfoDelta").collect()
parallelThreads = 25
result = parallelInt2(numbers,parallelThreads)
