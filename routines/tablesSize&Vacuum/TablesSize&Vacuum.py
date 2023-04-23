# Databricks notebook source
# DBTITLE 1,Levantamento de volumetrias das tabelas Deltas e aplicação de Vacuum
# MAGIC %md
# MAGIC | Versão | data | Descrição |
# MAGIC |-----------|-------|----------|
# MAGIC | `v1.0` | 2022-12-01 | Executando em clientes internos |
# MAGIC | `v1.1` | 2023-02-25 | Liberado para mais alguns clientes e engenheiros|
# MAGIC | `v2.0` | 2023-04-24 | Liberado publicamente |
# MAGIC 
# MAGIC Link do post: https://www.datainaction.dev/post/databricks-tablessize-vacuum-monitore-e-reduza-custos-do-seu-delta-lake
# MAGIC 
# MAGIC <h1>Funcionalidade e objetivo </h1>
# MAGIC 
# MAGIC > Esse notebook tem como principal objetivo coletar informações de tamanho e realizar limpeza das tabelas no formato Delta.
# MAGIC >
# MAGIC > São coletadas informações do tamanho da tabela no **Storage** e cruzadas com o tamanho da **versão atual**, assim podemos estimar quanto de espaço a operação de Vacuum poderia liberar.
# MAGIC >
# MAGIC > **Nota**: Focado para ambientes sem Unity Catalog ainda, embora funcione, o script será adaptado para ambientes com Unity Catalog
# MAGIC >
# MAGIC > **Nota 2**: As primeiras execuções do Vacuum podem demorar mais se seu ambiente nunca passou por essa manutenção, as execuções posteriores serão mais rápidas, pois, menos tabelas precisarão de vacuum, isso conforme o parâmetro vacuumThreshold.
# MAGIC >
# MAGIC > **Nota 3**: Reforçando, a primeira execução rode com o parâmetro **runVacuum = False**, apenas para você avaliar e ter uma noção de como está seu ambiente e quanto tempo a rotina irá levar.
# MAGIC >
# MAGIC > **Nota 4**: Se você sofrer algum erro, me envie pelo Github, LinkeDin ou por e-mail e posso te ajudar: reginaldo.silva27@gmail.com
# MAGIC >
# MAGIC > **Nota 5**: Pode aumentar o custo do seu Storage em relação às transações devido às chamadas do DButils.fs.ls, por isso, use com cautela, monitore e rode no máximo 1 vez por semana.
# MAGIC >
# MAGIC > **Nota 6**: Testes realizados com Azure (Usando o caminho absoluto e Mount), AWS (Mount) e GCP (Mount) para gravar as tabelas de controle.
# MAGIC >
# MAGIC > Abaixo são os passos executados em orderm de execução:
# MAGIC 1. Listagem de todas as tabelas existentes para determinado database ou grupo de databases, usamos um SHOW DATABASES E SHOW TABLES
# MAGIC 2. Executado um **describe detail** para cada tabela e armazenado o resultado em uma tabela Delta para análise e monitoramento
# MAGIC 3. É executado uma varredura (dbutils.fs.ls) nas pastas do Storage recursivamente para calcular o espaço ocupado no Storage por cada tabela, excluindo os _delta_logs
# MAGIC 4. Executado queries de análise para avaliar quais tabelas podem se beneficiar do Vacuum
# MAGIC 5. Executado a operação de Vacuum nas tabelas que atingem o threshold definido
# MAGIC 
# MAGIC **Recomendação de Cluster:** <br>
# MAGIC > Comece com um cluster pequeno e monitore, cluster inicial: **Driver: Standard_DS4_v2 · 2 x Workers: Standard_DS3_v2 · Runtime >11.3**
# MAGIC 
# MAGIC **Observações:** <br>
# MAGIC - **Existem outras formas de realizar essa operação, no entanto, essa foi a menos complexa e com melhor performance com exceção da operação Vacuum DRY RUN em Scala**<br>
# MAGIC - **A primeira versão desenvolvida utilizei a leitura dos Delta Logs, Jsons e checkpoints, contudo, não consegui reproduzir exatamente a operação de Vacuum DRY RUN e a performance ficou pior devido a quantidade de validações que precisei adicionar**<br>
# MAGIC   - Nessa versão mapiei todos os arquivos marcados como Remove no log, embora, seja mais performático a precisão não era (dados não batiam) por alguns fatores, para contornar esses fatores o script ficou mais lento
# MAGIC   - Tentei reproduzir via Scala, contudo, meu conhecimento em Scala é limitado e ficou muito complexo
# MAGIC   - Falei com alguns engenheiros da comunidade Delta, mas não tive sucesso em evoluir via Scala
# MAGIC - **Se você rodar o Vaccum Dry Run via Scala ele printa o retorno, contudo, esse retorno vai para o Stdout e ficou muito complexo de recuperar**<br>
# MAGIC ``%scala
# MAGIC vacuum tablename dry run``
# MAGIC - **Estou avaliando uma nova versão com delta.rs**
# MAGIC   - referencia: <https://github.com/delta-io/delta-rs/blob/main/python/docs/source/usage.rst>
# MAGIC 
# MAGIC Caso você queira se aventurar com Scala, aqui está o código-fonte:<br>
# MAGIC <https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>
# MAGIC <br>
# MAGIC Se você conseguir levantar essa quantidade de espaço de forma mais performática, me atualiza via comentário ou no Github.
# MAGIC 
# MAGIC **Ponto de atenção:** <br>
# MAGIC - **Para tabelas particionadas com muitas partições o tempo de execução pode ser mais demorado, por isso monitore as primeiras execuções com cautela, o uso é de sua responsabilidade, apesar de não ter nenhum risco mapeado até o momento, apenas pode gerar mais transações para sua storage**<br>
# MAGIC - **Custo das transações do Storage no Azure: Read operations (per 10,000) - R$0.0258 (Dois centavos por 10 mil operações) (Preço estimado em 21/04/2023)**
# MAGIC 
# MAGIC <br><h2>Descriçao dos parametros </h2>
# MAGIC 
# MAGIC ### Parametros de controle
# MAGIC 
# MAGIC | Parametro | Valor | Descrição |
# MAGIC |-----------|-------|----------|
# MAGIC | `numThreadsParallel` | **15** | Número de threads paralelas, avalie o melhor valor para o seu ambiente, faça testes |
# MAGIC | `vacuumThreadsParallel` | **5** | Número de threads paralelas para execução do **Vacuum**, utilize um valor menor, pois, pode dar problema no cluster, avalie o melhor valor para o seu ambiente, faça testes |
# MAGIC | `runVacuum` | **False** | Se definido como **True** executa o Vacuum com os parâmetros configurados, o valor padrão é **False** execute a primeira vez no default para ter uma noção de como está o seu ambiente |
# MAGIC | `vacuumHours` | **168** | Quantidade de horas que será mantido de versões após o Vacuum, defina o melhor para o seu ambiente, o padrão é 7 dias |
# MAGIC | `vacuumThreshold` | **5x** | Executar o Vacuum apenas nas tabelas em que o storage for **5x** maior do que a versão atual, exemplo: **Uma tabela que tem 100GB de dados na versão atual e possuir 500GB no Storage, irá entrar na rotina de limpeza** |
# MAGIC | `enableLogs` | **False** | Se definido como **True** irá gerar logs para facilitar algumas análises como, por exemplo, o tempo de duração para cada tabela e o erro se houver, contudo,nível eleva bastante o tempo de processamento se você tiver muitas tabelas |
# MAGIC | `enableHistory` | **False** | Se definido como **True** mantém histórico de todas as execuções da rotina e cada execução possuirá um identificador único para poder relacionar entre as tabelas, se definido como **False** (valor padrão) as tabelas de logs (tableDetails e tableStorageFiles) sempre serão truncadas |
# MAGIC 
# MAGIC ### Parametros para definição dos metadados
# MAGIC 
# MAGIC | Parametro | Valor | Descrição |
# MAGIC |-----------|-------|----------|
# MAGIC | `databaseTarget` | 'bronze*' | Define quais bancos de dados serão analisados, aceita regex com **, exemplo: todos os databases que começam com a palavra bronze: 'bronze*' |
# MAGIC | `tablesTarget` | '*' |  Definir quais tabelas serão analisadas, aceita regex com **, por padrão todas serão analisadas |
# MAGIC | `databaseCatalog` | 'db_controle' | Define em qual database será armazenado os logs, caso não exista será criado um novo |
# MAGIC | `tableCatalog` | 'tbCatalog' | Define nome da tabela de controle para armazenar as tabelas que serão analisadas, caso não exista a tabela será criada |
# MAGIC | `tbVacuumSummary` | 'tbVacuumSummary' | Define nome da tabela para armazenar o resultado agregado da execução, caso não exista a tabela será criada |
# MAGIC | `tablesSizeMonitor` | 'tablesSizeMonitor' | Define nome da tabela para armazenar o resultado agregado da execução com detalhes no nível de tabela, caso não exista a tabela será criada |
# MAGIC | `tableDetails` | 'bdaTablesDetails' | Define nome da tabela que irá armazenar o resultado do describe detail, caso não exista a tabela será criada |
# MAGIC | `tableStorageFiles` | 'bdaTablesStorageSize' | Define nome da tabela que irá armazenar o resultado do dbutils.fs.ls |
# MAGIC | `storageLocation` | 'abfss://[container]@[Storage].dfs.core.windows.net/pastaraiz/' [**Exemplo no Azure**]| Define endereço de storage principal, pode ser usado o valor absoluto ou um Mount (dbfs:/mnt/bronze/pastaRaiz/) |
# MAGIC | `tableCatalogLocation` | f'database=db_controle/table_name={tableCatalog}' | Define storage da tabela de catálogo |
# MAGIC | `tablesdetailsLocation` | f'database=db_controle/table_name={tableDetails}' | Define storage da tabela de detalhes do describe |
# MAGIC | `tableStorageFilesLocation` | f'database=db_controle/table_name={tableStorageFiles}' | Define storage da tabela de resultado do dbutils |
# MAGIC | `writeMode` | "overwrite" | Modo de escrita, "append" se `enableHistory` é verdadeiro, caso contrário "overwrite" |
# MAGIC | `identifier` | str(hash(datetime.today())) | Identificador unico para cada execução, usado para vincular as tabelas com suas devidas execuções |
# MAGIC 
# MAGIC ## Objetos criados: 1 database e 5x tabelas
# MAGIC 
# MAGIC > 1x Database nomeado através da variáve databaseCatalog, por padrão o nome será **db_controle** <br>
# MAGIC > 1x Tabela de catalogo, irá armazenar a listagem, por padrão o nome será **tbCatalog**, se o parâmetro enableHistory estiver desabilitado ela será sobrescrita em cada execução <br>
# MAGIC > 1x Tabela para armazenar o resultado do describe detail, por padrão será chamada de **bdaTablesDetails**, se o parâmetro enableHistory estiver desabilitado ela será sobrescrita em cada execução <br>
# MAGIC > 1x Tabela para armazenar o resultado do List files, por padrão será chamada de **tableStorageFiles**, se o parâmetro enableHistory estiver desabilitado ela será sobrescrita em cada execução <br>
# MAGIC > 1x Tabela para armazenar o resultado agregado da execução com detalhes no nivel de tabela, por padrão será chamada de **tablesSizeMonitor**, essa tabela nunca é truncada <br>
# MAGIC > 1x Tabela para armazenar o resultado agregado da execução, por padrão será chamada de **tbVacuumSummary**, essa tabela nunca é truncada <br>
# MAGIC 
# MAGIC ## Monitoramento
# MAGIC 
# MAGIC > Monitore seu ambiente através das tabelas tbVacuumSummary e tablesSizeMonitor<br>
# MAGIC > A tabela **tbVacuumSummary** armazena 1 linha por execução de dados sumarizados<br>
# MAGIC > A tabela **tablesSizeMonitor** armazena 1 linha por tabela por execução com dados sumarizados<br>
# MAGIC 
# MAGIC 
# MAGIC ## Benchmarch:
# MAGIC 
# MAGIC > Ambiente com mais de 3 mil tabelas - 200 TB de Storage - 12 horas - Cluster (1xnode DS5) - 50 Threads para analysis e 10 para vacuum - **Sem logs (enableLogs=False)** - Primeira execução<br>
# MAGIC > Ambiente com 300 tabelas - 5 TB de Storage - 1 hora - Cluster (1xnode DS3) - 25 Threads para analysis e 10 para vacuum - **Sem logs (enableLogs=False)** - Primeira execução<br>
# MAGIC > Ambiente com 300 tabelas - 5 TB de Storage - 2 horas - Cluster (1xnode DS3) - 25 Threads para analysis e 10 para vacuum - **Com logs (enableLogs=True)** - Primeira execução<br>
# MAGIC > Ambiente com 1000 tabelas - 10 GB de Storage - **6 horas - Cluster (1xnode DS3)** - 25 Threads para analysis e 10 para vacuum - **Com logs (enableLogs=True)** - Primeira execução<br>
# MAGIC Ambiente com 1000 tabelas - 10 GB de Storage - 6 horas - Cluster (1xnode DS3) - 25 Threads para analysis e 10 para vacuum - **Sem Logs (enableLogs=False)** - Primeira execução
# MAGIC 
# MAGIC ## Cases reais:
# MAGIC 
# MAGIC > **Case 1 - Azure:** Liberado mais de 250 TB na primeira execução em um ambiente que não havia rotina<br>
# MAGIC > **Case 2 - GCP:** Liberado 5 TB de logs em um ambiente pequeno, onde o total de dados eram apenas 50 GB e o storage tinha 5 TB<br>
# MAGIC > **Case 3 - Azure:** Liberado em média 10 TB de logs por semana, utilizando em um Databricks Job com agendamento para todos os finais de semana<br>
# MAGIC 
# MAGIC ## Implementações futuras:
# MAGIC 
# MAGIC > 1. Utilizar Unity Catalog<br>
# MAGIC > 2. Converter código para uma Lib em python<br>
# MAGIC > 3. Refatorar código usando a Lib e melhorando a usabilidade
# MAGIC > 4. Minimizar custos com dbutils.fs.ls, olhando direto para o transction log<br>
# MAGIC 
# MAGIC ## Referencias:
# MAGIC 
# MAGIC <https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>
# MAGIC 
# MAGIC <https://docs.databricks.com/sql/language-manual/delta-vacuum.html>
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > ``Criado por: Reginaldo Silva``
# MAGIC   - [Blog Data In Action](https://datainaction.dev/)
# MAGIC   - [Github](https://github.com/reginaldosilva27)

# COMMAND ----------

# DBTITLE 1,Declaração de parâmetros
# Cria Widgets para informar parametros (Usando Jobs ou Via Interface do Notebook)
# Ajuste parametros Default preenchidos caso necessários
# Preencha os parametros em branco via Job parameters ou via a interface liberada apos executar o comando abaixo, irá aparecer alguns Widgets
dbutils.widgets.text('databaseTarget',        '')
dbutils.widgets.text('tablesTarget',          '')
dbutils.widgets.text('storageLocation',       '')

##---- Default -----##
dbutils.widgets.text('numThreadsParallel',    '15')
dbutils.widgets.text('vacuumThreadsParallel', '5')
dbutils.widgets.text('runVacuum',             'False')
dbutils.widgets.text('vacuumHours',           '168')
dbutils.widgets.text('vacuumThreshold',       '5')
dbutils.widgets.text('enableLogs',            'False')
dbutils.widgets.text('enableHistory',         'False')
dbutils.widgets.text('databaseCatalog',       'db_controle')
dbutils.widgets.text('tableCatalog',          'tbCatalog')
dbutils.widgets.text('tbVacuumSummary',       'tbVacuumSummary')
dbutils.widgets.text('tablesSizeMonitor',     'tablesSizeMonitor')
dbutils.widgets.text('tableDetails',          'bdaTablesDetails')
dbutils.widgets.text('tableStorageFiles',     'bdaTablesStorageSize')

# COMMAND ----------

# DBTITLE 1,Parâmetros de execução
# Utiliza Parametros informados, se estiver rodando o notebook manualmente os valores do CMD2 ou dos Widgets serão utilizados
# Senão os valores enviados via Job parameters serão utilizados.
databaseTarget  =  dbutils.widgets.get('databaseTarget')
tablesTarget    =  dbutils.widgets.get('tablesTarget')
storageLocation =  dbutils.widgets.get('storageLocation')

# COMMAND ----------

# DBTITLE 1,Parâmetros de configuração
# Utiliza Parametros informados, se estiver rodando o notebook manualmente os valores do CMD2 ou dos Widgets serão utilizados
# Senão os valores enviados via Job parameters serão utilizados.
numThreadsParallel    = int(dbutils.widgets.get('numThreadsParallel'))
vacuumThreadsParallel = int(dbutils.widgets.get('vacuumThreadsParallel'))
runVacuum             = dbutils.widgets.get('runVacuum') == 'True'
vacuumHours           = int(dbutils.widgets.get('vacuumHours'))
vacuumThreshold       = int(dbutils.widgets.get('vacuumThreshold'))
enableLogs            = dbutils.widgets.get('enableLogs') == 'True'
enableHistory         = dbutils.widgets.get('enableHistory') == 'True'
databaseCatalog       = dbutils.widgets.get('databaseCatalog')
tableCatalog          = dbutils.widgets.get('tableCatalog')
tableDetails          = dbutils.widgets.get('tableDetails')
tableStorageFiles     = dbutils.widgets.get('tableStorageFiles')
tbVacuumSummary       = dbutils.widgets.get('tbVacuumSummary')
tablesSizeMonitor     = dbutils.widgets.get('tablesSizeMonitor')

# COMMAND ----------

# DBTITLE 1,Definição de variáveis
from datetime import datetime,date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,round,lit
from concurrent.futures import ThreadPoolExecutor

tableCatalogLocation = f'database={databaseCatalog}/table_name={tableCatalog}'           # Definir storage da tabela de catalogo
tablesdetailsLocation = f'database={databaseCatalog}/table_name={tableDetails}'          # Definir storage da tabela de detalhes do describe
tableStorageFilesLocation = f'database={databaseCatalog}/table_name={tableStorageFiles}' # Definir storage da tabela de resultado do dbutils
tbVacuumSummaryLocation = f'database={databaseCatalog}/table_name={tbVacuumSummary}' # Definir storage da tabela de resultado do dbutils
tablesSizeMonitorLocation = f'database={databaseCatalog}/table_name={tablesSizeMonitor}' # Definir storage da tabela de resultado do dbutils

writeMode = "append" if enableHistory else "overwrite"
identifier = str(hash(datetime.today()))
                                                                  
print(f"Database Target       : {databaseTarget}")
print(f"Tables Target         : {tablesTarget}")
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
        return [ec.submit(vacuum.run, table.databaseName, table.tableName, vacuumHours) for table in tables]
    
# Função para listar pastas de forma recursiva, excluindo a pasta _delta_log
def getDirContent(ls_path):
    path_list = dbutils.fs.ls(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isDir() and ls_path != dir_path.path and '_delta_log' not in dir_path.path:
            path_list += getDirContent(dir_path.path)
    return path_list

# COMMAND ----------

# DBTITLE 1,Tabelas de monitoramento
spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {databaseCatalog}.{tbVacuumSummary} (
          identifier STRING,
          dateLoad date,
          runVacuum STRING,
          enableLogs STRING, 
          enableHistory STRING,
          numThreadsParallel INT,
          vacuumThreadsParallel INT,
          vacuumHours INT,
          startAnalysis TIMESTAMP,
          endAnalysis TIMESTAMP,
          qtdDatabases INT,
          qtdTables INT,
          storageSizeGB DOUBLE,
          actualTablesSizeGB DOUBLE,
          qtdTablesVacuum INT,
          startVacuum TIMESTAMP,
          endVacuum TIMESTAMP
          )
        USING delta
        PARTITIONED BY (dateLoad)
        LOCATION '{storageLocation}{tbVacuumSummaryLocation}'
        """)

spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {databaseCatalog}.{tablesSizeMonitor} (
          identifier STRING,
          dateLoad date,
          databaseName STRING,
          tableName STRING,
          storageSizeGB DOUBLE,
          storageQtdFiles INT,
          actualTablesSizeGB DOUBLE,
          actualTableFiles INT,
          diffGB INT,
          storageVsTable INT,
          status STRING
          )
        USING delta
        PARTITIONED BY (dateLoad)
        LOCATION '{storageLocation}{tablesSizeMonitorLocation}'
        """)

# COMMAND ----------

# DBTITLE 1,Insere log de inicio
spark.sql(f"""INSERT INTO {databaseCatalog}.{tbVacuumSummary} VALUES ('{identifier}',CURRENT_DATE,'{runVacuum}','{enableLogs}','{enableHistory}',{numThreadsParallel},{vacuumThreadsParallel},{vacuumHours},CURRENT_TIMESTAMP,NULL,0,0,0,0,0,NULL,NULL)""")

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
        df = spark.sql(f"show tables in {db.databaseName} like '{tablesTarget}'")
        df2 = spark.sql(f"show views in {db.databaseName} like '{tablesTarget}'")
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
    ##Atualiza Logs
    spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET qtdDatabases = {countDB} where identifier = '{identifier}' """)
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
    spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET qtdTables = {len(listTables)} where identifier = '{identifier}' """)
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
    
    listDetailTables = spark.sql(f"select distinct location,name as tableName from {databaseCatalog}.{tableDetails} where identifier = '{identifier}' and numFiles > 0").collect()
    
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

# DBTITLE 1,Chamada da função listStorage
listStorage()

# COMMAND ----------

# DBTITLE 1,Query para agregar dados
spark.sql(f"""
INSERT INTO {databaseCatalog}.{tablesSizeMonitor}
select
  t.identifier,
  current_date as dateLoad,
  SPLIT(REPLACE(f.tableName,'spark_catalog.',''),'[.]')[0] databaseName,
  SPLIT(REPLACE(f.tableName,'spark_catalog.',''),'[.]')[1] tableName,
  f.storageSizeGB,
  f.storageQtdFiles,
  round(((t.sizeInBytes / 1024) / 1024) / 1024, 2) as actualtableSizeGB,
  t.numFiles as actualTableFiles,
  round(storageSizeGB - ((t.sizeInBytes / 1024) / 1024) / 1024,2) as diffGB,
  cast(f.storageSizeGB / (t.sizeInBytes / 1024 / 1024 / 1024) as int) as StorageVsTable,
  concat('Storage is bigger ',cast(cast(f.storageSizeGB / (t.sizeInBytes / 1024 / 1024 / 1024) as int) as string),'x (times)') as status
from
  {databaseCatalog}.{tableDetails} t
  left join (
    select
      identifier,
      tableName,
      Round(Sum(SizeMB), 2) as storageSizeMB,
      Round(Sum(SizeMB) / 1024, 2) as storageSizeGB,
      Count(*) storageQtdFiles
    from {databaseCatalog}.{tableStorageFiles} where dateload >= current_date() and identifier = {identifier}
    group by identifier,tableName
  ) f on f.tableName = t.name and f.identifier = t.identifier
where
  dateload >= current_date()   
  and t.status <> 'error'
  and tableName is not null
  and t.id is not null
  and t.identifier = {identifier}
""")

# COMMAND ----------

# DBTITLE 1,Query para totalizar o tamanho do storage vs tabelas
storageSizeGB = spark.sql(f""" select round(sum(storageSizeGB),2) as storageSizeGB from {databaseCatalog}.{tablesSizeMonitor} WHERE identifier = {identifier}""").collect()[0][0]
actualTablesSizeGB = spark.sql(f""" select round(sum(actualTablesSizeGB),2) as actualtableSizeGB from {databaseCatalog}.{tablesSizeMonitor} WHERE identifier = {identifier}""").collect()[0][0]
qtdTablesVacuum = spark.sql(f""" select count(*) from {databaseCatalog}.{tablesSizeMonitor} WHERE identifier = {identifier} and StorageVsTable > {vacuumThreshold} """).collect()[0][0]

# COMMAND ----------

spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET storageSizeGB = {storageSizeGB}, actualTablesSizeGB = {actualTablesSizeGB}  where identifier = '{identifier}' """)

# COMMAND ----------

# DBTITLE 1,Função para executar a manutenção nas tabelas
# https://github.com/reginaldosilva27/Databricks/tree/main/routines/OptimizeAndVacuum
def maintenanceDeltalake (nomeSchema='silver', nomeTabela='none', colunasZorder='none', vacuumRetention=168, vacuum=True, optimize=True, debug=True):
    if debug:
        print("Modo Debug habilitado!")
        if optimize:            
            if colunasZorder != "none":
                print(f">>> Otimizando tabela {nomeSchema}.{nomeTabela} com ZORDER no grupo de colunas: {colunasZorder} <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {nomeSchema}.{nomeTabela} ZORDER BY ({colunasZorder})")
            else:
                print(f">>> Otimizando tabela {nomeSchema}.{nomeTabela} sem ZORDER <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {nomeSchema}.{nomeTabela}")
            print(f">>> Tabela {nomeSchema}.{nomeTabela} otimizada! <<< >>> {str(datetime.now())}")
        else:
            print(f"### Não executado OPTIMIZE! ###")
        
        if vacuum:
            print(f">>> Setando {vacuumRetention} horas para limpeza de versionamento do deltalake... <<< >>> {str(datetime.now())}")
            print(f"CMD: VACUUM {nomeSchema}.{nomeTabela} RETAIN {vacuumRetention} Hours")
            print(f">>> Limpeza da tabela {nomeSchema}.{nomeTabela} aplicada com sucesso! <<< >>> {str(datetime.now())}")
        else:
            print(f"### Não executado VACUUM! ###")
    else:
        if optimize:
            if colunasZorder != "none":
                print(f">>> Otimizando tabela {nomeSchema}.{nomeTabela} com ZORDER no grupo de colunas: {colunasZorder} <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {nomeSchema}.{nomeTabela} ZORDER BY ({colunasZorder})")
                spark.sql(f"OPTIMIZE {nomeSchema}.{nomeTabela} ZORDER BY ({colunasZorder})")
            else:
                print(f">>> Otimizando tabela {nomeSchema}.{nomeTabela} sem ZORDER <<< >>> {str(datetime.now())}")
                print(f"CMD: OPTIMIZE {nomeSchema}.{nomeTabela}")
                spark.sql(f"OPTIMIZE {nomeSchema}.{nomeTabela}")
            print(f">>> Tabela {nomeSchema}.{nomeTabela} otimizada! <<< >>> {str(datetime.now())}")
        if vacuum:
            spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
            print(f"CMD: VACUUM {nomeSchema}.{nomeTabela} RETAIN {vacuumRetention} Hours")
            spark.sql(f"VACUUM {nomeSchema}.{nomeTabela} RETAIN {vacuumRetention} Hours")
            spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")
            print(f">>> Limpeza da tabela {nomeSchema}.{nomeTabela} aplicada com sucesso! <<< >>> {str(datetime.now())}")

# COMMAND ----------

# DBTITLE 1,Classe para paralelizar as chamadas
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

spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET endAnalysis = CURRENT_TIMESTAMP, startVacuum = CURRENT_TIMESTAMP, qtdTablesVacuum = '{qtdTablesVacuum}'  where identifier = '{identifier}' """)

# COMMAND ----------

# DBTITLE 1,Executar operação de Vacuum com a função "maintenanceDeltalake"
if runVacuum:
    print(f"Iniciando a operação de Vacuum... {datetime.today()}")
    tables = spark.sql(f"""select databaseName, tableName from {databaseCatalog}.{tablesSizeMonitor} where StorageVsTable > {vacuumThreshold} and identifier = '{identifier}' """).collect()
    totalTables = len(tables)
    if totalTables > 0:
        print(f"Total de tabelas para executar vacuum: {totalTables}")
        try:            
            parallelVacuum(tables, vacuumThreadsParallel)            
        except Exception as e:
            print('oi')
            print(e)
            spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET endVacuum = CURRENT_TIMESTAMP  where identifier = '{identifier}' """)
    print(f"Operação de Vacuum finalizada - {datetime.today()}")
    spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET endVacuum = CURRENT_TIMESTAMP  where identifier = '{identifier}' """)
else:
    print(f"Operação Vacuum não configurada. {datetime.today()}")
    spark.sql(f"""UPDATE {databaseCatalog}.{tbVacuumSummary} SET endVacuum = CURRENT_TIMESTAMP  where identifier = '{identifier}' """)

# COMMAND ----------

# DBTITLE 1,Resumo da execução
spark.sql(f"""select * from {databaseCatalog}.{tbVacuumSummary} where identifier = '{identifier}' """).display()

# COMMAND ----------

# DBTITLE 1,Tabelas e Databases analisados
spark.sql(f'select * from {databaseCatalog}.{tablesSizeMonitor} WHERE identifier = {identifier}').display()
