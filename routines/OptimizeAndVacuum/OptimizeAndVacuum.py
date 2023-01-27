# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Descriçao dos parametros </h1>
# MAGIC 
# MAGIC | Parametro  | Descrição | Tipo
# MAGIC | ------------- | ------------- | ------------- |
# MAGIC | nomeSchema  | Nome do Database onde a tabela está criada  | string |
# MAGIC | nomeTabela  | Nome da tabela que será aplicado a manutenção  | string |
# MAGIC | vacuum  | True: Vacuum será executado, False: Pula vacuum  | bool |
# MAGIC | optimize  | True: OPTIMIZE será executado, False: Pula OPTIMIZE   | bool |
# MAGIC | colunasZorder  | Se informado e optimize for igual a True, aplicada Zorder na lista de colunas separado por vírgula (,)  | string |
# MAGIC | vacuumRetention  | Quantidade de horas que será retida após execucao do Vacuum  | integer |
# MAGIC | Debug  | Apenas imprime o resultado na tela  | bool |
# MAGIC 
# MAGIC <h2> Exemplos: </h2>
# MAGIC 
# MAGIC #### --> Primeiro instanciar a Function <--
# MAGIC `` %run /Users/reginaldo.silva@dataside.com.br/OptimizeAndVacuum ``
# MAGIC 
# MAGIC #### --> Executando VACUUM com retenção de 72 horas e OPTMIZE SEM ZORDER <--
# MAGIC ``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='funcionario', colunasZorder='none', vacuumRetention=72, vacuum=True, optimize=True, debug=False)``
# MAGIC 
# MAGIC #### --> Executando VACUUM retenção padrão e OPTMIZE COM ZORDER <--
# MAGIC ``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='patient_id', vacuumRetention=168, vacuum=True, optimize=True, debug=False)``
# MAGIC 
# MAGIC #### --> Executando somente VACUUM <--
# MAGIC ``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='none', vacuumRetention=168, vacuum=True, optimize=False, debug=False)``
# MAGIC 
# MAGIC #### --> Executando somente OPTMIZE <--
# MAGIC ``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='none', vacuumRetention=168, vacuum=False, optimize=True, debug=False)``
# MAGIC 
# MAGIC #### --> Modo Debug - Apenas print <--
# MAGIC ``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='none', vacuumRetention=168, vacuum=True, optimize=True, debug=True)``
# MAGIC 
# MAGIC >``Criado por: Reginaldo Silva``
# MAGIC 
# MAGIC [Blog Data In Action](https://datainaction.dev/)
# MAGIC 
# MAGIC [Github](https://github.com/reginaldosilva27)

# COMMAND ----------

from datetime import datetime
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
        print("Modo Debug desabilitado!")
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
        else:
            print(f"### Não executado OPTIMIZE! ###")
        
        if vacuum:
            print(f">>> Setando {vacuumRetention} horas para limpeza de versionamento do deltalake... <<< >>> {str(datetime.now())}")
            spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
            print(f"CMD: VACUUM {nomeSchema}.{nomeTabela} RETAIN {vacuumRetention} Hours")
            spark.sql(f"VACUUM {nomeSchema}.{nomeTabela} RETAIN {vacuumRetention} Hours")
            spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")
            print(f">>> Limpeza da tabela {nomeSchema}.{nomeTabela} aplicada com sucesso! <<< >>> {str(datetime.now())}")
        else:
            print(f"### Não executado VACUUM! ###")

# COMMAND ----------

# DBTITLE 1,Enviar parâmetros para execução após instanciar a função
# MAGIC %py 
# MAGIC #Caso queira já chamar a função diretamente do Azure Data Factory, informar os parametros na chamada do notebook
# MAGIC try:
# MAGIC     maintenanceDeltalake(nomeSchema=getArgument("NomeSchema"), nomeTabela=getArgument("NomeTabela"), colunasZorder=getArgument("ColunasZorder"), vacuumRetention=getArgument("VacuumRetention"), vacuum=eval(getArgument("Vacuum")), optimize=eval(getArgument("Optimize")), debug=eval(getArgument("Debug")))
# MAGIC except:
# MAGIC     print("Função maintenanceDeltalake() instanciada no contexto!")
