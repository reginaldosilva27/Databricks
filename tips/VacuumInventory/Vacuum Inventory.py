# Databricks notebook source
# DBTITLE 1,Variables
from datetime import datetime, timedelta
## Script for Azure Databricks
## For AWS and GCP you need customize some code blocks
## If you are not using Unity Catalog, use as catalog name: hive_metastore
## Author: Reginaldo Silva

##########################
## Set Variables
##########################
storageName = 'sahierarchicaldatalake'
dataBucket = 'datalake'
inventoryBucket = 'inventory'

inventoryCatalogName = 'dev'
inventoryDabaseName = 'datainaction'
inventoryTableName = 'vacuumInventory'
##########################

current_day = datetime.now().strftime('%d')
current_month = datetime.now().strftime('%m') 
current_year = datetime.now().year
dataStoragePath = f'abfss://{dataBucket}@{storageName}.dfs.core.windows.net/'

try:
  dbutils.fs.ls(f'abfss://{inventoryBucket}@{storageName}.dfs.core.windows.net/{current_year}/{current_month}/{current_day}/')
except: 
  print('No files found using current day, trying D-1...')
  try:
    current_day = (datetime.today() + timedelta(days=-1)).strftime('%d')
    dbutils.fs.ls(f'abfss://{inventoryBucket}@{storageName}.dfs.core.windows.net/{current_year}/{current_month}/{current_day}/')
    print('Using D-1!')
  except:
    print('No files found!')
    dbutils.notebook.exit('No files found in inventory folder!')

inventoryStoragePath = f'abfss://{inventoryBucket}@{storageName}.dfs.core.windows.net/{current_year}/{current_month}/{current_day}/*/*/*.parquet'

print('Inventory Storage path: ', inventoryStoragePath)
print('Data Storage path: ', dataStoragePath)
print(f'Inventory Table: {inventoryCatalogName}.{inventoryDabaseName}.{inventoryTableName}')

# COMMAND ----------

# DBTITLE 1,Create Inventory Table
spark.sql(f""" 
CREATE TABLE IF NOT EXISTS {inventoryCatalogName}.{inventoryDabaseName}.{inventoryTableName}
(
  path string, 
  `creationTime` long,
  `modificationTime` long,
  `length` long,
  `isDir` boolean,
  `LastAccessTime` string,
  SourceFileName string not null,
  datetimeLoad timestamp
);
"""
)

# COMMAND ----------

# DBTITLE 1,Clean data
# Clean inventory table, we will load new updated data
spark.sql(f""" 
Truncate table {inventoryCatalogName}.{inventoryDabaseName}.{inventoryTableName}
"""
)

# COMMAND ----------

# DBTITLE 1,INSERT INTO inventory table
# Insert new inventory Data to delta table
# Get just the necessary fields
# Field hdi_isfolder is another Option to achive isDir field
spark.sql(f""" 
INSERT INTO {inventoryCatalogName}.{inventoryDabaseName}.{inventoryTableName}
select
      concat('{dataStoragePath}',replace(name,'{dataBucket}/','')) as path,
      `Creation-Time` creationTime,
      `Last-Modified` modificationTime,
      `Content-Length` as length,
      case when `Content-Length` > 0 then false else true end isDir,
      cast(from_unixtime(`LastAccessTime` / 1000) as string) LastAccessTime,
      _metadata.file_name as SourceFileName,
      current_timestamp as datetimeLoad
    from
      parquet.`{inventoryStoragePath}`
"""
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC > Example
# MAGIC >> **vacuum catalog.database.tableName using inventory (select path, length, isDir, modificationTime from dev.datainaction.vacuumInventory) RETAIN 48 HOURS**
