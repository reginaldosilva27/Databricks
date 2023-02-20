# Databricks notebook source
# DBTITLE 1,Usando o Dbutils.fs.ls para listar uma pasta
dbutils.fs.ls('/databricks-datasets/COVID/')

# COMMAND ----------

# DBTITLE 1,Transformar Dbutils em Dataframe
# MAGIC %py
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# MAGIC 
# MAGIC ddlSchema = StructType([
# MAGIC StructField('path',StringType()),
# MAGIC StructField('name',StringType()),
# MAGIC StructField('size',IntegerType()),
# MAGIC StructField('modificationTime',StringType())
# MAGIC ])
# MAGIC 
# MAGIC ls = dbutils.fs.ls('/databricks-datasets/COVID/')
# MAGIC dfPath = spark.createDataFrame(ls,ddlSchema)
# MAGIC dfPath.createOrReplaceTempView('vw_Files')

# COMMAND ----------

# DBTITLE 1,Consultando com SQL
# MAGIC %sql
# MAGIC -- Note que temos apenas 2 arquivos os demais sao pastas
# MAGIC select count(*) qtdFiles,sum(size) / 1024 / 1024 as size_Mb from vw_Files where size > 0

# COMMAND ----------

# DBTITLE 1,Visualizando estrutura
# MAGIC %sql
# MAGIC -- As pastas sempre ficam com Size 0 mesmo tendo arquivos dentro
# MAGIC select * from vw_Files

# COMMAND ----------

# DBTITLE 1,Função recursiva para listar todos niveis de pasta
# Basicamente é uma função que chama ela mesma durante a execução
def get_dir_content(ls_path):
    path_list = dbutils.fs.ls(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isDir() and ls_path != dir_path.path:
            path_list += get_dir_content(dir_path.path)
    return path_list

# COMMAND ----------

# DBTITLE 1,Agora vamos usar nossa função para gerar o DataFrame
# MAGIC %py
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# MAGIC 
# MAGIC ddlSchema = StructType([
# MAGIC StructField('path',StringType()),
# MAGIC StructField('name',StringType()),
# MAGIC StructField('size',IntegerType()),
# MAGIC StructField('modificationTime',StringType())
# MAGIC ])
# MAGIC 
# MAGIC dfPath = spark.createDataFrame(get_dir_content('/databricks-datasets/COVID/covid-19-data'),ddlSchema)
# MAGIC dfPath.createOrReplaceTempView('vw_Files')

# COMMAND ----------

# DBTITLE 1,Agora temos todos os arquivos daquela pasta e subpastas
# MAGIC %sql
# MAGIC select count(*) qtdFiles,sum(size) / 1024 / 1024 as size_Mb from vw_Files where size > 0
