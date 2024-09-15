-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Tabela de migração: Estrategias por tipo de tabela
-- MAGIC
-- MAGIC | Id | Tipo HMS | Location       | Tipo UC            | Método                  |
-- MAGIC |----|----------|----------------|--------------------|--------------------------|
-- MAGIC | 1  | Managed  | DBFS Root      | Managed/External   | CTAS / DEEP CLONE        |
-- MAGIC | 2  | Managed  | DBFS Root      | Managed/External   | CTAS / DEEP CLONE        |
-- MAGIC | 3  | Hive SerDe  | DBFS Root      | Managed/External   | CTAS / DEEP CLONE        |
-- MAGIC | 4  | Managed  | Mount          | External           | SYNC com Convert         |
-- MAGIC | 5  | Managed  | Mount          | Managed            | CTAS / DEEP CLONE        |
-- MAGIC | 6  | External | Mount          | External           | SYNC                     |
-- MAGIC | 7  | External | Mount          | Managed            | CTAS / DEEP CLONE        |
-- MAGIC | 8  | Managed  | Cloud Storage  | External           | SYNC com Convert         |
-- MAGIC | 9  | Managed  | Cloud Storage  | Managed            | CTAS / DEEP CLONE        |
-- MAGIC | 10  | External | Cloud Storage  | External           | SYNC                     |
-- MAGIC | 11  | External | Cloud Storage  | Managed            | CTAS / DEEP CLONE        |
-- MAGIC
-- MAGIC ## Observação importante
-- MAGIC - **set spark.databricks.sync.command.enableManagedTable=true;**
-- MAGIC   - Ao usar essa opção, você não pode dropar a tabela no HMS, pois, o dados serão excluídos do Storage
-- MAGIC   - Caso queira dropar, use o script Scala para trocar ela de Managed para External
-- MAGIC
-- MAGIC ## Tabelas Managed vs External
-- MAGIC
-- MAGIC - **Tabelas Managed**:
-- MAGIC   - Dados e metadados são gerenciados pelo Unity Catalog.
-- MAGIC   - Os dados são armazenados no local especificado pelo catálogo Unity (tipicamente em armazenamento cloud).
-- MAGIC   - A exclusão de uma tabela managed remove também os dados.
-- MAGIC     - Se for HMS os dados são removidos imediatamente
-- MAGIC     - Se for no UC os dados são mantidos por mais 30 dias
-- MAGIC       - Aqui voce pode usar o UNDROP até 7 dias
-- MAGIC
-- MAGIC - **Tabelas External**:
-- MAGIC   - Apenas os metadados são gerenciados pelo Unity Catalog, os dados permanecem no armazenamento externo (geralmente em um bucket ou outro recurso cloud).
-- MAGIC   - A exclusão de uma tabela external remove apenas os metadados; os dados permanecem no armazenamento original.
-- MAGIC   - Permite que os dados sejam compartilhados entre diferentes sistemas ou aplicações.
-- MAGIC
-- MAGIC ### DBFS Root vs Mount vs Cloud Storage
-- MAGIC
-- MAGIC - **DBFS Root**:
-- MAGIC   - O sistema de arquivos distribuído do Databricks (Databricks File System).
-- MAGIC   - Armazenamento temporário e volátil, com possíveis limitações em operações de longa duração.
-- MAGIC   - Os dados ficam fisicamente no storage da Databricks que voce não tem acesso
-- MAGIC
-- MAGIC - **Mount**:
-- MAGIC   - Uma forma de acessar o armazenamento externo (como S3, ADLS) no DBFS como se fosse um diretório local.
-- MAGIC   - Os dados permanecem no armazenamento externo, mas podem ser acessados dentro de Databricks via caminhos montados.
-- MAGIC
-- MAGIC - **Cloud Storage**:
-- MAGIC   - Armazenamento na nuvem (ex: AWS S3, Azure Data Lake, Google Cloud Storage) onde os dados podem ser armazenados e acessados diretamente.
-- MAGIC   - Mais flexível para armazenamento de grande volume e soluções a longo prazo.
-- MAGIC
-- MAGIC ### Métodos CTAS, DEEP CLONE e SYNC
-- MAGIC
-- MAGIC - **CTAS (Create Table As Select)**:
-- MAGIC   - Método usado para criar uma nova tabela a partir dos resultados de uma consulta SQL.
-- MAGIC   - A nova tabela pode ser criada com dados agregados ou filtrados.
-- MAGIC   - Exemplo de uso: `CREATE TABLE nova_tabela AS SELECT * FROM tabela_existente WHERE condição`.
-- MAGIC
-- MAGIC - **DEEP CLONE**:
-- MAGIC   - Método utilizado para clonar tabelas, incluindo seus dados, metadados e histórico de transações.
-- MAGIC   - Utilizado para cópia rápida de tabelas, útil em cenários de backup ou migração.
-- MAGIC   - Exemplo: `DEEP CLONE origem DESTINO` cria uma cópia completa da tabela de origem.
-- MAGIC
-- MAGIC - **SYNC**:
-- MAGIC   - Sincroniza tabelas external com o Unity Catalog, garantindo que o catálogo reflita as alterações feitas diretamente no armazenamento.
-- MAGIC   - Essencial para manter a consistência entre os metadados no Unity Catalog e o armazenamento externo.
-- MAGIC   - Útil para cenários onde os dados podem ser alterados por fora do Databricks.
-- MAGIC
-- MAGIC
-- MAGIC Post Databricks:
-- MAGIC https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore#appendix
-- MAGIC
-- MAGIC Notebook oficial:
-- MAGIC   https://notebooks.databricks.com/notebooks/uc-upgrade-scenario-with-examples-for-blog.dbc?_gl=1*1nrxwtq*_gcl_au*OTUxMzE5NDg3LjE2OTM0NjcxNDM.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Scenario 1: Managed tables on HMS with DBFS Root location

-- COMMAND ----------

drop database if exists hive_metastore.hmsdb_upgrade_db cascade;
create database if not exists hive_metastore.hmsdb_upgrade_db;

-- COMMAND ----------

desc schema extended hive_metastore.hmsdb_upgrade_db;

-- COMMAND ----------

drop table if exists hive_metastore.hmsdb_upgrade_db.people_parquet;
create table if not exists hive_metastore.hmsdb_upgrade_db.people_parquet
using parquet
as
select * from parquet.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.parquet/` limit 100;

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_parquet;

-- COMMAND ----------

drop schema if exists demo_uc_demo.uc_upgrade_db cascade;

-- COMMAND ----------

create schema if not exists demo_uc_demo.uc_upgrade_db managed location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/demo_uc/uc_upgrade_db";

-- COMMAND ----------

-- DBTITLE 1,Create UC managed Delta table using CTAS (Preferred)
-- Pq não deep clone? DEEP CLONE é recomendado para tabelas DELTA
drop table if exists demo_uc_demo.uc_upgrade_db.people_delta;
create table if not exists 
demo_uc_demo.uc_upgrade_db.people_delta
as
select * from hive_metastore.hmsdb_upgrade_db.people_parquet;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

-- DBTITLE 1,Alternatively Create UC External table (with the same HMS file format) using CTAS
drop table if exists demo_uc_demo.uc_upgrade_db.people_parquet_ext;
create table if not exists demo_uc_demo.uc_upgrade_db.people_parquet_ext
using parquet
location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/demo_uc/uc_upgrade_db/people_parquet_ext"
as
select * from hive_metastore.hmsdb_upgrade_db.people_parquet;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_parquet_ext;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Scenario 2: External tables on HMS with DBFS Root location

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 3: HMS Hive SerDe table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 4: Managed table on HMS with mounted file paths to External UC Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mounts()

-- COMMAND ----------

drop database if exists hive_metastore.hmsdb_upgrade_db cascade;
create database if not exists hive_metastore.hmsdb_upgrade_db location "dbfs:/mnt/landing/hmsdb_upgrade_db/"

-- COMMAND ----------

-- DBTITLE 1,Managed Delta HMS table
drop table if exists hive_metastore.hmsdb_upgrade_db.people_delta;
create table if not exists hive_metastore.hmsdb_upgrade_db.people_delta
as
select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 100;

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

select current_version();

-- COMMAND ----------

drop schema if exists demo_uc_demo.uc_upgrade_db cascade;
create schema if not exists demo_uc_demo.uc_upgrade_db managed location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/demo_uc/uc_upgrade_db";

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta DRY RUN;

-- COMMAND ----------

set spark.databricks.sync.command.enableManagedTable=true;

-- COMMAND ----------

describe extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

select * from demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

-- DBTITLE 1,Convert HMS Managed Table to External Table
-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
-- MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
-- MAGIC
-- MAGIC val tableName = "people_delta"
-- MAGIC val dbName = "hmsdb_upgrade_db"
-- MAGIC
-- MAGIC val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(dbName)))
-- MAGIC val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.EXTERNAL)
-- MAGIC spark.sessionState.catalog.alterTable(alteredTable)

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

-- Arquivos não serão apagados
drop table hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

select * from demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta DRY RUN;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 5: Managed table on HMS with mounted file paths to Managed UC Table

-- COMMAND ----------

drop database if exists hive_metastore.hmsdb_upgrade_db cascade;
create database if not exists hive_metastore.hmsdb_upgrade_db location "dbfs:/mnt/landing/hmsdb_upgrade_db/"

-- COMMAND ----------

drop table if exists hive_metastore.hmsdb_upgrade_db.people_delta;
create table if not exists hive_metastore.hmsdb_upgrade_db.people_delta
as
select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 100;

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

drop schema if exists demo_uc_demo.uc_upgrade_db cascade;
create schema if not exists demo_uc_demo.uc_upgrade_db managed location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/demo_uc/uc_upgrade_db/uc_upgrade_schema_2/";

-- COMMAND ----------

set spark.databricks.sync.command.enableManagedTable=false;

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta DRY RUN;

-- COMMAND ----------

-- OU DEEP CLONE
drop table if exists demo_uc_demo.uc_upgrade_db.people_delta;
create table if not exists demo_uc_demo.uc_upgrade_db.people_delta
as
select * from hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 6: External table on HMS with mounted file paths to External UC Table

-- COMMAND ----------

drop database if exists hive_metastore.hmsdb_upgrade_db cascade;
create database if not exists hive_metastore.hmsdb_upgrade_db location "dbfs:/mnt/landing/hmsdb_upgrade_db/"

-- COMMAND ----------

drop table if exists hive_metastore.hmsdb_upgrade_db.people_delta;
create table if not exists hive_metastore.hmsdb_upgrade_db.people_delta
location "dbfs:/mnt/landing/hmsdb_upgrade_db/people_delta"
as
select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 100;

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

drop schema if exists demo_uc_demo.uc_upgrade_db cascade;
create schema if not exists demo_uc_demo.uc_upgrade_db managed location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/uc_upgrade_schema_2/";

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta DRY RUN;

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 7: External table on HMS with mounted file paths to Managed UC Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 8: Managed table on HMS with cloud storage file paths to External UC Table

-- COMMAND ----------

drop database if exists hive_metastore.hmsdb_upgrade_db cascade;
create database if not exists hive_metastore.hmsdb_upgrade_db location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/hmsdb_upgrade_db/"

-- COMMAND ----------

drop table if exists hive_metastore.hmsdb_upgrade_db.people_delta;
create table if not exists hive_metastore.hmsdb_upgrade_db.people_delta
as
select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 100;

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

set spark.databricks.sync.command.enableManagedTable=true;

-- COMMAND ----------

drop schema if exists demo_uc_demo.uc_upgrade_db cascade;
create schema if not exists demo_uc_demo.uc_upgrade_db managed location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/uc_upgrade_schema_10/";

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta DRY RUN;

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
-- MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
-- MAGIC
-- MAGIC val tableName = "people_delta"
-- MAGIC val dbName = "hmsdb_upgrade_db"
-- MAGIC
-- MAGIC val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(dbName)))
-- MAGIC val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.EXTERNAL)
-- MAGIC spark.sessionState.catalog.alterTable(alteredTable)

-- COMMAND ----------

select * from demo_uc_demo.uc_upgrade_db.people_delta ;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 9: Managed table on HMS with cloud storage file paths to Managed UC Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 10: External table on HMS with cloud storage file paths to External UC Table

-- COMMAND ----------

drop database if exists hive_metastore.hmsdb_upgrade_db cascade;
create database if not exists hive_metastore.hmsdb_upgrade_db location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/hms/hmsdb_upgrade_db/"

-- COMMAND ----------

drop table if exists hive_metastore.hmsdb_upgrade_db.people_delta;
create table if not exists hive_metastore.hmsdb_upgrade_db.people_delta
location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/hms/hmsdb_upgrade_db/people_delta"
as
select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 100;

-- COMMAND ----------

desc extended hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

create catalog if not exists demo_uc_demo

-- COMMAND ----------

drop schema if exists demo_uc_demo.uc_upgrade_db cascade;
create schema if not exists demo_uc_demo.uc_upgrade_db managed location "abfss://bronze@datalakedatainactiondev.dfs.core.windows.net/unitycatalog/demo_uc_demo/uc_upgrade_db/";

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta DRY RUN;

-- COMMAND ----------

sync table demo_uc_demo.uc_upgrade_db.people_delta from hive_metastore.hmsdb_upgrade_db.people_delta;

-- COMMAND ----------

desc extended demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

select * from demo_uc_demo.uc_upgrade_db.people_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Scenario 11: External table on HMS with cloud storage file paths to Managed UC Table
