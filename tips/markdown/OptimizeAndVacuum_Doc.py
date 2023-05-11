# Databricks notebook source
# DBTITLE 1,Exemplo de markdown com imagem
# MAGIC %md
# MAGIC | **Coluna1**     | **Coluna2**       | **Coluna3** |
# MAGIC | --------------- | ------------------| ----------- |
# MAGIC | `linha1`        | Desc1.            | `str`       |
# MAGIC | `linha2`        | Desc2.            | `str`       |
# MAGIC | `linha3`        | Desc3.            | `str`       |
# MAGIC
# MAGIC ![Nome da imagem](https://static.wixstatic.com/media/a794bc_cb3926c8de254beea142cc8cb2c40e58~mv2.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC # `maintenanceDeltalake`
# MAGIC
# MAGIC A função `maintenanceDeltalake` é utilizada para executar operações de manutenção em uma tabela Delta Lake. Ela oferece a opção de otimizar a tabela e executar a limpeza de versionamento.
# MAGIC
# MAGIC ## Parâmetros
# MAGIC
# MAGIC | **Nome**            | **Descrição**                                                                         | **Tipo**    |
# MAGIC | ------------------- | ------------------------------------------------------------------------------------- | ----------- |
# MAGIC | `nomeSchema`        | O nome do esquema (schema) da tabela Delta Lake.                                       | `str`       |
# MAGIC | `nomeTabela`        | O nome da tabela Delta Lake.                                                          | `str`       |
# MAGIC | `colunasZorder`     | O grupo de colunas para aplicar o ZORDER.                                              | `str`       |
# MAGIC | `vacuumRetention`   | O tempo de retenção em horas para a limpeza de versionamento.                          | `int`       |
# MAGIC | `vacuum`            | Indica se a limpeza de versionamento deve ser executada.                               | `bool`      |
# MAGIC | `optimize`          | Indica se a otimização da tabela deve ser executada.                                   | `bool`      |
# MAGIC | `debug`             | Indica se o modo de depuração está habilitado.                                         | `bool`      |
# MAGIC
# MAGIC ## Exemplo de Uso
# MAGIC
# MAGIC Aqui estão três exemplos de chamadas da função `maintenanceDeltalake` com diferentes parâmetros:
# MAGIC
# MAGIC 1. Exemplo com otimização e limpeza habilitadas:
# MAGIC ```python
# MAGIC maintenanceDeltalake(nomeSchema='silver', nomeTabela='my_table', colunasZorder='column1, column2', vacuumRetention=72, vacuum=True, optimize=True, debug=True)
# MAGIC ```
# MAGIC <br>2. Exemplo sem otimização e com limpeza desabilitada:
# MAGIC ```python
# MAGIC maintenanceDeltalake(nomeSchema='silver', nomeTabela='my_table', colunasZorder='none', vacuumRetention=168, vacuum=False, optimize=False, debug=True)
# MAGIC ```
# MAGIC <br>3. Exemplo sem otimização e limpeza habilitadas, em modo de produção:
# MAGIC ```python
# MAGIC maintenanceDeltalake(nomeSchema='silver', nomeTabela='my_table', colunasZorder='none', vacuumRetention=168, vacuum=True, optimize=False, debug=False)
# MAGIC ```
# MAGIC
# MAGIC >Observação: Lembre-se de fornecer os valores corretos para os parâmetros, com base nas suas necessidades específicas.
# MAGIC
# MAGIC Referência
# MAGIC Para obter mais informações sobre como otimizar seu Delta Lake e reduzir os custos de storage e computação no Databricks, confira o seguinte post: Otimize seu Delta Lake e reduza custos de storage, Databricks e computação.

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

# MAGIC %md
# MAGIC Enviar parâmetros para execução após instanciar a função

# COMMAND ----------

# DBTITLE 1,Enviar parâmetros para execução após instanciar a função
# MAGIC %py 
# MAGIC #Caso queira já chamar a função diretamente do Azure Data Factory, informar os parametros na chamada do notebook
# MAGIC try:
# MAGIC     maintenanceDeltalake(nomeSchema=getArgument("NomeSchema"), nomeTabela=getArgument("NomeTabela"), colunasZorder=getArgument("ColunasZorder"), vacuumRetention=getArgument("VacuumRetention"), vacuum=eval(getArgument("Vacuum")), optimize=eval(getArgument("Optimize")), debug=eval(getArgument("Debug")))
# MAGIC except:
# MAGIC     print("Função maintenanceDeltalake() instanciada no contexto!")
