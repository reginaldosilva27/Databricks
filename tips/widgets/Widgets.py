# Databricks notebook source
# DBTITLE 1,Definindo manualmente um Widget do tipo texto
#Define Widgets
dbutils.widgets.text('path',      '')
dbutils.widgets.text('dataini',   '')
dbutils.widgets.text('datafim',   '')
dbutils.widgets.dropdown('debug', 'False', ['True','False'])

# Define variaveis
path    =  dbutils.widgets.get('path')
dataini =  dbutils.widgets.get('dataini')
datafim =  dbutils.widgets.get('datafim')
debug   =  dbutils.widgets.get('debug') == 'True' # Retorna um boolean

# Se for modo Debug apenas mostra o valor das variaveis, senao executa um comando, nesse caso o dbutils
if(debug):
  print('path   : ',path)
  print('dataini: ',dataini)
  print('datafim: ',datafim)
else:
  dbutils.fs.ls(path)

# COMMAND ----------

# DBTITLE 1,Chamando uma função usando uma váriavel
def getDirContent(ls_path):
    path_list = dbutils.fs.ls(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isDir() and ls_path != dir_path.path and '_delta_log' not in dir_path.path:
            path_list += getDirContent(dir_path.path)
  
getDirContent(path)

# COMMAND ----------

# DBTITLE 1,Chamando uma função com valor fixo
def getDirContent(ls_path):
    path_list = dbutils.fs.ls(ls_path)
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isDir() and ls_path != dir_path.path and '_delta_log' not in dir_path.path:
            path_list += getDirContent(dir_path.path)
  
getDirContent('/databricks-datasets/COVID/USAFacts/')

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Veja as opções de Widgets
dbutils.widgets.help()
