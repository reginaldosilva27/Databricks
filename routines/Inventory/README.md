<h1>Funcionalidade e objetivo </h1>

> Esse notebook tem como principal objetivo coletar informações e realizar limpeza das tabelas no formato Delta.
> 
> São coletadas informações do tamanho da tabela no Storage e cruzado com o tamanho da versão atual, assim podemos estimar quanto de espaço a operação de Vacuum poderá liberar.
>
> Abaixo são os passos executados em orderm de execução:
1. Listagem de todas as tabelas existente para determinado database ou grupo de databases
2. Executado um describe detail para cada tabela e armazenado o resultado em uma tabela Delta para analise e monitoramento
3. É executado uma varredura nas pastas do Storage para calcular o espaço ocupado por cada tabela, excluindo os _delta_logs
4. Executado queries de analise
5. Executado a operação de Vacuum nas tabelas que atingem o threshold definido

**Observações:** <br>
- **Existem outras formas de realizar essa operação, no entanto, essa foi a menos complexa e com melhor performance com exceção da operação Vacuum DRY RUN em Scala**<br>
- **A primeira versão desenvolvida fiz pela leitura dos Delta Logs, Jsons e checkpoints, contudo, não consegui reproduzir exatamente a operação de Vacuum DRY RUN e a performance ficou pior**<br>
  - Nessa versão mapiei todos os arquivos marcados como Remove no log, embora, seja mais performático a precisão não era por alguns fatores, para contornar esses fatores o script ficou mais lento
  - Tentei reproduzir via Scala, contudo, meu conhecimento em Scala é limitado e ficou muito complexo
  - Falei com alguns engenheiros da comunidade Delta, mas não tive sucesso em evoluir via Scala
- **Se você rodar o Vaccum Dry Rum via Scala ele printa o retorno, contudo, esse retorno vai para o Stdout e ficou muito complexo de recuperar**<br>
``%scala
vacuum tablename dry run``

Caso você queria se aventurar com Scala, aqui esta o código fonte:<br>
<https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>
<br>
Se você conseguir levantar essa quantidade de espaço de forma mais performatica ou mesmo conseguir decifrar o código Scala, me atualiza via comentário ou no Github.

<br><h2>Descriçao dos parametros </h2>

### Parametros de controle

| Parametro | Valor | Descrição |
|-----------|-------|----------|
| `numThreadsParallel` | **25** | Número de threads paralelas, avalie o melhor valor para o seu ambiente, faça testes |
| `vacuumThreadsParallel` | **10** | Número de threads paralelas para execução do **Vacuum**, utilize um valor menor, pois, pode dar problema no cluster, avalie o melhor valor para o seu ambiente, faça testes |
| `runVacuum` | **False** | Se definido como **True** executa o Vacuum com os parametros configurados, o valor padrão é **False** execute a primeira vez no default para ter uma noção de como esta o seu ambiente |
| `vacuumHours` | **168** | Quantidade de horas que será mantido de versões após o Vacuum, defina o melhor para o seu ambiente, o padrão é 7 dias |
| `vacuumThreshold` | **30** | Executar o Vacuum apenas nas tabelas que tiverem **30%** (ou o valor que você informar) mais logs do que dados, exemplo: **Uma tabela que tem 100GB de dados na versão atual e possuir 130GB no Storage, irá entrar na rotina de limpeza** |
| `enableLogs` | **False** | Se definido como **True** irá gerar logs para facilitar algumas análises como por exemplo o tempo de duração para cada tabela e o erro se houver, contudo eleva bastante o tempo de processamento se você tiver muitas tabelas |
| `enableHistory` | **False** | Se definido como **True** mantém histórico de todas as execuções da rotina, se definido como **False** (valor padrão) as tabelas sempre serão truncadas |

### Parametros para definição dos metadados

| Parametro | Valor | Descrição |
|-----------|-------|----------|
| `databaseTarget` | 'bronze*' | Define quais bancos de dados serão analisados, aceita regex com **, exemplo: todos os databases que começam com a palavra bronze: 'bronze*' |
| `databaseCatalog` | 'db_controle' | Define em qual database será armazenado os logs, caso não exista será criado um novo |
| `tableCatalog` | 'tbCatalog' | Define nome da tabela de controle para armazenar as tabelas que serão analisadas, caso não exista a tabela será criada |
| `tableDetails` | 'bdaTablesDetails' | Define nome da tabela que irá armazenar o resultado do describe detail, caso não exista a tabela será criada |
| `tableStorageFiles` | 'bdaTablesStorageSize' | Define nome da tabela que irá armazenar o resultado do dbutils.fs.ls |
| `storageLocation` | 'abfss://[container]@[Storage].dfs.core.windows.net/' | Define endereço de storage principal, pode ser usado o valor absoluto, como um Mount (dbfs:/mnt/bronze/), funciona em qualquer Cloud, se devidamente autenticado |
| `tableCatalogLocation` | f'database=db_controle/table_name={tableCatalog}' | Define storage da tabela de catálogo |
| `tablesdetailsLocation` | f'database=db_controle/table_name={tableDetails}' | Define storage da tabela de detalhes do describe |
| `tableStorageFilesLocation` | f'database=db_controle/table_name={tableStorageFiles}' | Define storage da tabela de resultado do dbutils |
| `writeMode` | "overwrite" | Modo de escrita, "append" se `enableHistory` é verdadeiro, caso contrário "overwrite" |
| `identifier` | str(hash(datetime.today())) | Identificador unico para cada execução, usado para vincular as tabelas com suas devidas execuções |


### Dependencias:
> Ajuste o comando 14 do Notebook, corrija o caminho do notebook: %run /Databricks/OptimizeAndVacuum
<br><https://github.com/reginaldosilva27/Databricks/tree/main/routines/OptimizeAndVacuum>

## Referencias:

<https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>

<https://docs.databricks.com/sql/language-manual/delta-vacuum.html>
