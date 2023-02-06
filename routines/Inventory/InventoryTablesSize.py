# Databricks notebook source
# DBTITLE 1,Levantamento de volumetrias das tabelas Deltas
# MAGIC %md
# MAGIC <h1>Funcionalidade e objetivo </h1>
# MAGIC 
# MAGIC > Esse notebook tem como principal objetivo coletar informações e realizar limpeza das tabelas no formato Delta.
# MAGIC > 
# MAGIC > São coletadas informações do tamanho da tabela no Storage e cruzado com o tamanho da versão atual, assim podemos estimar quanto de espaço a operação de Vacuum poderá liberar.
# MAGIC >
# MAGIC > Abaixo são os passos executados em orderm de execução:
# MAGIC 1. Listagem de todas as tabelas existente para determinado database ou grupo de databases
# MAGIC 2. Executado um describe detail para cada tabela e armazenado o resultado em uma tabela Delta para analise e monitoramento
# MAGIC 3. É executado uma varredura nas pastas do Storage para calcular o espaço ocupado por cada tabela, excluindo os _delta_logs
# MAGIC 4. Executado queries de analise
# MAGIC 5. Executado a operação de Vacuum nas tabelas que atingem o threshold definido
# MAGIC 
# MAGIC **Observações:** <br>
# MAGIC - **Existem outras formas de realizar essa operação, no entanto, essa foi a menos complexa e com melhor performance com exceção da operação Vacuum DRY RUN em Scala**<br>
# MAGIC - **A primeira versão desenvolvida fiz pela leitura dos Delta Logs, Jsons e checkpoints, contudo, não consegui reproduzir exatamente a operação de Vacuum DRY RUN e a performance ficou pior**<br>
# MAGIC   - Nessa versão mapiei todos os arquivos marcados como Remove no log, embora, seja mais performático a precisão não era por alguns fatores, para contornar esses fatores o script ficou mais lento
# MAGIC   - Tentei reproduzir via Scala, contudo, meu conhecimento em Scala é limitado e ficou muito complexo
# MAGIC   - Falei com alguns engenheiros da comunidade Delta, mas não tive sucesso em evoluir via Scala
# MAGIC - **Se você rodar o Vaccum Dry Rum via Scala ele printa o retorno, contudo, esse retorno vai para o Stdout e ficou muito complexo de recuperar**<br>
# MAGIC ``%scala
# MAGIC vacuum tablename dry run``
# MAGIC 
# MAGIC Caso você queria se aventurar com Scala, aqui esta o código fonte:<br>
# MAGIC <https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>
# MAGIC <br>
# MAGIC Se você conseguir levantar essa quantidade de espaço de forma mais performatica ou mesmo conseguir decifrar o código Scala, me atualiza via comentário ou no Github.
# MAGIC 
# MAGIC <br><h2>Descriçao dos parametros </h2>
# MAGIC 
# MAGIC ### Parametros de controle
# MAGIC 
# MAGIC | Parametro | Valor | Descrição |
# MAGIC |-----------|-------|----------|
# MAGIC | `numThreadsParallel` | **25** | Número de threads paralelas, avalie o melhor valor para o seu ambiente, faça testes |
# MAGIC | `vacuumThreadsParallel` | **10** | Número de threads paralelas para execução do **Vacuum**, utilize um valor menor, pois, pode dar problema no cluster, avalie o melhor valor para o seu ambiente, faça testes |
# MAGIC | `runVacuum` | **False** | Se definido como **True** executa o Vacuum com os parametros configurados, o valor padrão é **False** execute a primeira vez no default para ter uma noção de como esta o seu ambiente |
# MAGIC | `vacuumHours` | **168** | Quantidade de horas que será mantido de versões após o Vacuum, defina o melhor para o seu ambiente, o padrão é 7 dias |
# MAGIC | `vacuumThreshold` | **30** | Executar o Vacuum apenas nas tabelas que tiverem **30%** (ou o valor que você informar) mais logs do que dados, exemplo: **Uma tabela que tem 100GB de dados na versão atual e possuir 130GB no Storage, irá entrar na rotina de limpeza** |
# MAGIC | `enableLogs` | **False** | Se definido como **True** irá gerar logs para facilitar algumas análises como por exemplo o tempo de duração para cada tabela e o erro se houver, contudo eleva bastante o tempo de processamento se você tiver muitas tabelas |
# MAGIC | `enableHistory` | **False** | Se definido como **True** mantém histórico de todas as execuções da rotina, se definido como **False** (valor padrão) as tabelas sempre serão truncadas |
# MAGIC 
# MAGIC ### Parametros para definição dos metadados
# MAGIC 
# MAGIC | Parametro | Valor | Descrição |
# MAGIC |-----------|-------|----------|
# MAGIC | `databaseTarget` | 'bronze*' | Define quais bancos de dados serão analisados, aceita regex com **, exemplo: todos os databases que começam com a palavra bronze: 'bronze*' |
# MAGIC | `databaseCatalog` | 'db_controle' | Define em qual database será armazenado os logs, caso não exista será criado um novo |
# MAGIC | `tableCatalog` | 'tbCatalog' | Define nome da tabela de controle para armazenar as tabelas que serão analisadas, caso não exista a tabela será criada |
# MAGIC | `tableDetails` | 'bdaTablesDetails' | Define nome da tabela que irá armazenar o resultado do describe detail, caso não exista a tabela será criada |
# MAGIC | `tableStorageFiles` | 'bdaTablesStorageSize' | Define nome da tabela que irá armazenar o resultado do dbutils.fs.ls |
# MAGIC | `storageLocation` | 'abfss://[container]@[Storage].dfs.core.windows.net/' | Define endereço de storage principal, pode ser usado o valor absoluto, como um Mount (dbfs:/mnt/bronze/), funciona em qualquer Cloud, se devidamente autenticado |
# MAGIC | `tableCatalogLocation` | f'database=db_controle/table_name={tableCatalog}' | Define storage da tabela de catálogo |
# MAGIC | `tablesdetailsLocation` | f'database=db_controle/table_name={tableDetails}' | Define storage da tabela de detalhes do describe |
# MAGIC | `tableStorageFilesLocation` | f'database=db_controle/table_name={tableStorageFiles}' | Define storage da tabela de resultado do dbutils |
# MAGIC | `writeMode` | "overwrite" | Modo de escrita, "append" se `enableHistory` é verdadeiro, caso contrário "overwrite" |
# MAGIC | `identifier` | str(hash(datetime.today())) | Identificador unico para cada execução, usado para vincular as tabelas com suas devidas execuções |
# MAGIC 
# MAGIC 
# MAGIC ### Dependencias:
# MAGIC > Ajuste o comando 14 do Notebook, corrija o caminho do notebook: %run /Databricks/OptimizeAndVacuum
# MAGIC <br><https://github.com/reginaldosilva27/Databricks/tree/main/routines/OptimizeAndVacuum>
# MAGIC 
# MAGIC ##Referencias:
# MAGIC 
# MAGIC <https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>
# MAGIC 
# MAGIC <https://docs.databricks.com/sql/language-manual/delta-vacuum.html>

# COMMAND ----------

# DBTITLE 1,Definição de variáveis
from datetime import datetime,date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,round,lit
from concurrent.futures import ThreadPoolExecutor

### Parametros de controle ###
numThreadsParallel = 25                                                                  # Número de threads paralelas, avalie o melhor valor para o seu ambiente, faça testes 
vacuumThreadsParallel = 10                                                               # Número de threads paralelas para execução do **Vacuum**, utilize um valor menor, pois, pode dar problema no cluster, avalie o melhor valor para o seu ambiente, faça testes
runVacuum          = False                                                               # Se definido como True executa o Vacuum com os parametros configurados, o valor padrão é **False** execute a primeira vez no default para ter uma noção de como esta o seu ambiente
vacuumHours        = 168                                                                 # Quantidade de horas que será mantido de versões após o Vacuum, defina o melhor para o seu ambiente, o padrão é 7 dias
vacuumThreshold    = 30                                                                  # Executa Vacuum se o a quantidade que irá ser liberada é 30% maior do que o tamanho atual da tabela
enableLogs         = False                                                               # Gerar logs pode facilitar algumas analise, mas eleva o tempo de processamento
enableHistory      = False                                                               # Habilitar para manter histórico de todas as execuções, fique a vontade para definir uma rotina de limpeza das tabelas de controle

### Parametros para definição dos metadados ###
databaseTarget = 'bronze*'                                                               # Definir quais bancos de dados serão analisados, aceita regex com **, exemplo: todos os databases que começam com a palavra bronze: 'bronze*'
databaseCatalog = 'db_controle'                                                          # Definir em qual database será armazenado os logs, caso não exista será criado um novo
tableCatalog = 'tbCatalog'                                                               # Definir nome da tabela para armazenar as tabelas que serão analisadas, caso não exista a tabela será criada
tableDetails = 'bdaTablesDetails'                                                        # Definir nome da tabela que irá armazenar o resultado do describe detail, caso não exista a tabela será criada
tableStorageFiles = 'bdaTablesStorageSize'                                               # Definir nome da tabela que irá armazenar o resultado do dbutils.fs.ls
storageLocation = 'abfss://[container]@[Storage].dfs.core.windows.net'                   # Definir endereço de storage principal
tableCatalogLocation = f'database={databaseCatalog}/table_name={tableCatalog}'           # Definir storage da tabela de catalogo
tablesdetailsLocation = f'database={databaseCatalog}/table_name={tableDetails}'          # Definir storage da tabela de detalhes do describe
tableStorageFilesLocation = f'database={databaseCatalog}/table_name={tableStorageFiles}' # Definir storage da tabela de resultado do dbutils
writeMode = "append" if enableHistory else "overwrite"
identifier = str(hash(datetime.today()))
                                                                  
print(f"Database Target       : {databaseTarget}")
print(f"Table catalog         : {databaseCatalog}.{tableCatalog}")
print(f"Table Describe Details: {tableDetails}")
print(f"Table Storage Files   : {tableStorageFiles}")
print(f"Write mode            : {writeMode}")
print(f"Vacuum Enable         : {str(runVacuum)}")
print(f"Logs Enable           : {str(enableLogs)}")
print(f"History Enable        : {str(enableHistory)}")
print(f"ParallelThreads       : {str(numThreadsParallel)}")
print(f"Identificador         : {identifier}")

# COMMAND ----------

# DBTITLE 1,Define funções auxiliares
# Função para paralelizar as operações de Describe Detail
def parallelDescribe(tables, numInParallel):
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(describe.describeDetail, table.database, table.tableName) for table in tables]

# Função para paralelizar as operações listagem de arquivos no Storage
def parallelList(tables, numInParallel):
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(StorageSize.list, table.tableName, table.location) for table in tables]
    
# Função para paralelizar as operações listagem de arquivos no Storage
def parallelVacuum(tables, numInParallel):
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
        return [ec.submit(vacuum.run, table.database, table.tableName, vacuumHours) for table in tables]
    
# Função para listar pastas de forma recursiva, excluindo a pasta _delta_log
def getDirContent(ls_path):
    path_list = dbutils.fs.ls(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isDir() and ls_path != dir_path.path and '_delta_log' not in dir_path.path:
            path_list += getDirContent(dir_path.path)
    return path_list

# COMMAND ----------

# DBTITLE 1,Listar todas as tabelas do banco de dados parametrizado como target
def listTables():
    print(f"Iniciando a listagem de tabelas... {datetime.today()}")
    #Apenas instanciando os Datasets vazios
    dfTables = spark.sql("show tables in default like 'xxx'")
    dfViews = spark.sql("show views in default like 'xxx'")
    countDB = 0
    spark.sql(f"create database if not exists {databaseCatalog}")
    #Listando todos os databases e pegando todas as tabelas para o levantamento
    for db in spark.sql(f"show databases like '{databaseTarget}'").collect():
        countDB = countDB+1
        df = spark.sql(f"show tables in {db.databaseName}")
        df2 = spark.sql(f"show views in {db.databaseName}")
        dfTables = dfTables.union(df)
        dfViews = dfViews.union(df2)

    dfViews.createOrReplaceTempView('vw_views')
    dfTables.withColumn('identifier',lit(identifier)) \
    .withColumn('status',lit('pending')) \
    .withColumn('message',lit('')) \
    .withColumn('start',lit('')) \
    .withColumn('finish',lit('')) \
    .withColumn('dateLog',lit(f'{datetime.today()}')) \
    .withColumn('dateLoad',lit(f'{date.today()}')) \
    .write.mode(writeMode).partitionBy('tableName','database','dateLoad').saveAsTable(f"{databaseCatalog}.{tableCatalog}", path = f'{storageLocation}{tableCatalogLocation}')  
    
    spark.sql(f""" OPTIMIZE {databaseCatalog}.{tableCatalog}""")
    print(f"Total de Databases: {countDB} - Tabelas e views: {str(dfTables.count())} - {datetime.today()}")

# COMMAND ----------

# DBTITLE 1,Classe describe, utilizada para auxiliar no paralelismo das operações de describe
class describe:
    def __init__(self, database, tableName):
        self.database = database
        self.tableName = tableName
   
    def describeDetail(database, tableName):
        try:
            print(f'<<<<<<< Starting table {tableName} at {datetime.today()} <<<<<<<')
            if enableLogs: spark.sql(f"""update {databaseCatalog}.{tableCatalog} set start = current_timestamp() where tableName = '{tableName}' and database = '{database}' """)   
            
            df_detail = spark.sql(f"describe detail {database}.{tableName}")
            df_detail.withColumn('identifier',lit(identifier)) \
            .withColumn('status',lit('pending')) \
            .withColumn('message',lit('')) \
            .withColumn('start',lit('')) \
            .withColumn('finish',lit('')) \
            .withColumn('dateLog',lit(f'{datetime.today()}')) \
            .withColumn('dateLoad',lit(f'{date.today()}')) \
            .write.mode('append').partitionBy('name','dateLoad').option("overwriteSchema", True).saveAsTable(f"{databaseCatalog}.{tableDetails}", path = f'{storageLocation}{tablesdetailsLocation}')  
            
            if enableLogs: spark.sql(f"""update {databaseCatalog}.{tableCatalog} set status = 'success', message = "ok", finish = current_timestamp() where tableName = '{tableName}' and database = '{database}' """)
            
            print(f'>>>>>>> finished table {tableName} at {datetime.today()} >>>>>>>')            
        except Exception as e:
            print (f"###### Error to load tableName {tableName} at {datetime.today()} {e}######")
            if enableLogs: spark.sql(f"""update {databaseCatalog}.{tableCatalog} set status = 'error', message = "{e}",finish = current_timestamp() where tableName = '{tableName}' and database = '{database}' """)

# COMMAND ----------

# DBTITLE 1,Função para executar o Describe detail em paralelo
def describeDetail():
    listTables = spark.sql(f"select distinct database,tableName from {databaseCatalog}.{tableCatalog} where identifier = '{identifier}' and database <> '' and isTemporary = false and tableName not in (select v.viewName from vw_views v)").collect()  
    print(f"Iniciando a captura de detalhes para cada tabela... {datetime.today()}")
    print(f"Tabelas: {len(listTables)} - ParallelThreads: {numThreadsParallel}")    
    if not enableHistory:
        try:
            spark.sql(f"truncate table {databaseCatalog}.{tableDetails}")
        except:
            pass
    try:
        spark.sql(f""" 
        CREATE TABLE IF NOT EXISTS {databaseCatalog}.{tableDetails} (
          format STRING,
          id STRING,
          name STRING,
          description STRING,
          location STRING,
          createdAt TIMESTAMP,
          lastModified TIMESTAMP,
          partitionColumns ARRAY<STRING>,
          numFiles BIGINT,
          sizeInBytes BIGINT,
          properties MAP<STRING, STRING>,
          minReaderVersion INT,
          minWriterVersion INT,
          enabledTableFeatures ARRAY<STRING>,
          statistics MAP<STRING, BIGINT>,
          identifier STRING,
          status STRING,
          message STRING,
          start STRING,
          finish STRING,
          dateLog STRING,
          dateLoad STRING)
        USING delta
        PARTITIONED BY (name, dateLoad)
        LOCATION '{storageLocation}{tablesdetailsLocation}'
        """)
        parallelDescribe(listTables, numThreadsParallel)
        spark.sql(f"OPTIMIZE {databaseCatalog}.{tableDetails}")
    except Exception as e:
        print(e)

# COMMAND ----------

# DBTITLE 1,Classe StorageSize, utilizada para auxiliar no paralelismo das operações do storage
class StorageSize:
    def __init__(self, tableName, location):
        self.number = tableName
        self.location = location

    def list(tableName, location):        
        ddlSchema = StructType([
        StructField('path',StringType()),
        StructField('name',StringType()),
        StructField('size',IntegerType()),
        StructField('modificationTime',StringType())
        ])
    
        try:
            print(f'<<<<<<< Starting table {tableName} at {datetime.today()} <<<<<<<')
            if enableLogs: spark.sql(f"""update {databaseCatalog}.{tableDetails} set start = current_timestamp() where name = '{tableName}' """)            
            dfPath = spark.createDataFrame(getDirContent(location),ddlSchema)
            
            dfPath.withColumn('identifier',lit(identifier)) \
            .withColumn("modificationTime",(col('modificationTime') / 1000).cast('timestamp')) \
            .withColumn("sizeKB",round(col('size')/1024,2)) \
            .withColumn("sizeMB",round(col('size')/1024/1024,2)) \
            .withColumn("tableName", lit(tableName)) \
            .withColumn('dateLoad',lit(f'{date.today()}')) \
            .filter(col('size') > 0).write.mode('append').option("overwriteSchema", True).saveAsTable(f"{databaseCatalog}.{tableStorageFiles}", path = f'{storageLocation}{tableStorageFilesLocation}')
            
            if enableLogs: spark.sql(f"""update {databaseCatalog}.{tableDetails} set status = 'success', message = "ok", finish = current_timestamp() where name = '{tableName}' """)
            
            print(f'>>>>>>> finished table {tableName} at {datetime.today()} >>>>>>>')
        except Exception as e:
            print(f"##### Error to load tableName {tableName} at {datetime.today()} {e}#####")
            if enableLogs: spark.sql(f"""update {databaseCatalog}.{tableDetails} set status = 'error', message = "{e}",finish = current_timestamp() where name = '{tableName}' """)

# COMMAND ----------

# DBTITLE 1,Função para executar a listagem de arquivos em paralelo
def listStorage():
    print(f"Iniciando a listagem do storage para cada tabela... {datetime.today()}")
    if not enableHistory:
        try:
            spark.sql(f"truncate table {databaseCatalog}.{tableStorageFiles}")
        except:
            pass
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {databaseCatalog}.{tableStorageFiles} (
          identifier STRING,
          tableName STRING,
          path STRING,
          name STRING,
          size INT,
          sizeKB DOUBLE,
          sizeMB DOUBLE,
          modificationTime TIMESTAMP,
          dateLoad STRING)
        USING delta
        PARTITIONED BY (tableName, dateLoad)
        LOCATION '{storageLocation}{tableStorageFilesLocation}'
        """)
    
    listDetailTables = spark.sql(f"select distinct location,name as tableName from {databaseCatalog}.{tableDetails} where identifier = '{identifier}'").collect()
    
    print(f"Tabelas: {len(listDetailTables)} - ParallelThreads: {numThreadsParallel}") 
    try:            
        parallelList(listDetailTables, numThreadsParallel)
        spark.sql(f"OPTIMIZE {databaseCatalog}.{tableStorageFiles}")
    except Exception as e:
        print(e)

# COMMAND ----------

# DBTITLE 1,Chamada da função listTables e Select nos dados
listTables()
spark.sql(f"select * from {databaseCatalog}.{tableCatalog} where identifier = '{identifier}'").display()

# COMMAND ----------

# DBTITLE 1,Chamada da função describeTable e Select nos dados
describeDetail()
spark.sql(f"select * from {databaseCatalog}.{tableDetails} where identifier = '{identifier}'").display()

# COMMAND ----------

# DBTITLE 1,Chamada da função listStorage e Select nos dados
listStorage()
spark.sql(f"select * from {databaseCatalog}.{tableStorageFiles} where identifier = '{identifier}'").display()

# COMMAND ----------

# DBTITLE 1,Query para agregar dados
spark.sql(f"""
select
  f.tableName,
  f.storageSizeMB,
  round((t.sizeInBytes / 1024) / 1024, 2) as actualtableSizeMB,
  f.storageSizeGB,
  round(((t.sizeInBytes / 1024) / 1024) / 1024, 2) as actualtableSizeGB,
  f.storageQtdFiles,
  t.numFiles as actualTableFiles,
  round(storageSizeGB - ((t.sizeInBytes / 1024) / 1024) / 1024,2) as diffGB,
  100 - round(((t.sizeInBytes / 1024 / 1024 / 1024) / f.storageSizeGB) * 100 ,0) as  percentHistory
from
  {databaseCatalog}.{tableDetails} t
  left join (
    select
      identifier,
      tableName,
      Round(Sum(SizeMB), 2) as storageSizeMB,
      Round(Sum(SizeMB) / 1024, 2) as storageSizeGB,
      Count(*) storageQtdFiles
    from {databaseCatalog}.{tableStorageFiles}
    group by identifier,tableName
  ) f on f.tableName = t.name and f.identifier = t.identifier
where
  t.status <> 'error'
  and tableName is not null
  and t.id is not null
  and t.identifier = {identifier}
  and storageSizeGB > 5
order by diffGB desc
""").createOrReplaceTempView('vwVacuum')
spark.sql('select * from vwVacuum').display()

# COMMAND ----------

# DBTITLE 1,Query para totalizar o tamanho do storage vs tabela
spark.sql(f"""
select
  round(sum(storageSizeGB),2) as storageSizeGB,
  round(sum(actualtableSizeGB),2) as actualtableSizeGB,
  round(sum(storageSizeGB) - sum(actualtableSizeGB),2) as estimatedSaveGB,
  count(*) as qtdTables
from
  vwVacuum
where
  percentHistory > {vacuumThreshold}
  """).display()

# COMMAND ----------

# DBTITLE 1,Instanciar notebook OptimizeAndVacuum e função
# MAGIC %run /Databricks/OptimizeAndVacuum

# COMMAND ----------

class vacuum:
    def __init__(self, database, tableName, numThreadsParallel):
        self.tableName = database
        self.tableName = tableName
        self.numThreadsParallel = numThreadsParallel
   
    def run(database, tableName, numThreadsParallel):
        try:          
            maintenanceDeltalake(nomeSchema=database, nomeTabela=tableName, colunasZorder='none', vacuumRetention=numThreadsParallel, vacuum=True, optimize=False, debug=False)           
        except Exception as e:
            print (f"###### Error to vacuum tableName {tableName} at {datetime.today()} {e}######")

# COMMAND ----------

# DBTITLE 1,executar operação de Vacuum com a função "maintenanceDeltalake"
if runVacuum:
    print(f"Iniciando a operação de Vacuum... {datetime.today()}")
    tables = spark.sql(f"""select split(replace(tableName,'.','#'),'#')[1] as database, split(replace(tableName,'.','#'),'#')[2] as tableName from vwVacuum where percentHistory > {vacuumThreshold}""").collect()
    totalTables = len(tables)
    if totalTables > 0:
        print(f"Total de tabelas para executar vacuum: {totalTables}")
        try:            
            parallelVacuum(tables, vacuumThreadsParallel)
        except Exception as e:
            print(e)
else:
    print(f"Operação Vacuum não configurada. {datetime.today()}")
