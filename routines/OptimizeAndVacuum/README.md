
<h1>Descriçao dos parametros </h1>

| Parametro  | Descrição | Tipo
| ------------- | ------------- | ------------- |
| nomeSchema  | Nome do Database onde a tabela está criada  | string |
| nomeTabela  | Nome da tabela que será aplicado a manutenção  | string |
| vacuum  | True: Vacuum será executado, False: Pula vacuum  | bool |
| optimize  | True: OPTIMIZE será executado, False: Pula OPTIMIZE   | bool |
| colunasZorder  | Se informado e optimize for igual a True, aplicada Zorder na lista de colunas separado por vírgula (,)  | string |
| vacuumRetention  | Quantidade de horas que será retida após execucao do Vacuum  | integer |
| Debug  | Apenas imprime o resultado na tela  | bool |

<h2> Exemplos: </h2>

#### --> Primeiro instanciar a Function <--
`` %run /Users/reginaldo.silva@dataside.com.br/OptimizeAndVacuum ``

#### --> Executando VACUUM com retenção de 72 horas e OPTMIZE SEM ZORDER <--
``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='funcionario', colunasZorder='none', vacuumRetention=72, vacuum=True, optimize=True, debug=False)``

#### --> Executando VACUUM retenção padrão e OPTMIZE COM ZORDER <--
``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='patient_id', vacuumRetention=168, vacuum=True, optimize=True, debug=False)``

#### --> Executando somente VACUUM <--
``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='none', vacuumRetention=168, vacuum=True, optimize=False, debug=False)``

#### --> Executando somente OPTMIZE <--
``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='none', vacuumRetention=168, vacuum=False, optimize=True, debug=False)``

#### --> Modo Debug - Apenas print <--
``maintenanceDeltalake(nomeSchema='db_festivaldemo', nomeTabela='PatientInfoDelta', colunasZorder='none', vacuumRetention=168, vacuum=True, optimize=True, debug=True)``

>``Criado por: Reginaldo Silva``

[Blog Data In Action](https://datainaction.dev/)

[Github](https://github.com/reginaldosilva27)
