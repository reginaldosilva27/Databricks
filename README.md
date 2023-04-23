# Databricks
Notebooks e dicas sobre Databricks
<br>
## Para usar os scripts desse Repos, basta importar para sua pasta no Databricks.
<br>
Selecionar o opção Import
<img width="355" alt="image" src="https://user-images.githubusercontent.com/69867503/218261423-276004c0-87d8-40f2-a37c-1d1dfc25f11d.png">
<br>
Selecionar o script e importar:
<img width="489" alt="image" src="https://user-images.githubusercontent.com/69867503/218261470-653f47d0-597d-4731-bec2-da14a1cce4b8.png">


| Versão | data | Descrição |
|-----------|-------|----------|
| `v1.0` | 2022-12-01 | Executando em clientes internos |
| `v1.1` | 2023-02-25 | Liberado para mais alguns clientes e engenheiros|
| `v2.0` | 2023-04-24 | Liberado publicamente |

<h1>Funcionalidade e objetivo </h1>

> Esse notebook tem como principal objetivo coletar informações de tamanho e realizar limpeza das tabelas no formato Delta.
>
> São coletadas informações do tamanho da tabela no **Storage** e cruzadas com o tamanho da **versão atual**, assim podemos estimar quanto de espaço a operação de Vacuum poderia liberar.
>
> **Nota**: Focado para ambientes sem Unity Catalog ainda, embora funcione, o script será adaptado para ambientes com Unity Catalog
>
> **Nota 2**: As primeiras execuções do Vacuum podem demorar mais se seu ambiente nunca passou por essa manutenção, as execuções posteriores serão mais rápidas, pois, menos tabelas precisarão de vacuum, isso conforme o parâmetro vacuumThreshold.
>
> **Nota 3**: Reforçando, a primeira execução rode com o parâmetro **runVacuum = False**, apenas para você avaliar e ter uma noção de como está seu ambiente e quanto tempo a rotina irá levar.
>
> **Nota 4**: Se você sofrer algum erro, me envie pelo Github, LinkeDin ou por e-mail e posso te ajudar: reginaldo.silva27@gmail.com
>
> **Nota 5**: Pode aumentar o custo do seu Storage em relação às transações devido às chamadas do DButils.fs.ls, por isso, use com cautela, monitore e rode no máximo 1 vez por semana.
>
> **Nota 6**: Testes realizados com Azure (Usando o caminho absoluto e Mount), AWS (Mount) e GCP (Mount) para gravar as tabelas de controle.
>
> Abaixo são os passos executados em orderm de execução:
1. Listagem de todas as tabelas existentes para determinado database ou grupo de databases, usamos um SHOW DATABASES E SHOW TABLES
2. Executado um **describe detail** para cada tabela e armazenado o resultado em uma tabela Delta para análise e monitoramento
3. É executado uma varredura (dbutils.fs.ls) nas pastas do Storage recursivamente para calcular o espaço ocupado no Storage por cada tabela, excluindo os _delta_logs
4. Executado queries de análise para avaliar quais tabelas podem se beneficiar do Vacuum
5. Executado a operação de Vacuum nas tabelas que atingem o threshold definido

**Recomendação de Cluster:** <br>
> Comece com um cluster pequeno e monitore, cluster inicial: **Driver: Standard_DS4_v2 · 2 x Workers: Standard_DS3_v2 · Runtime >11.3**

**Observações:** <br>
- **Existem outras formas de realizar essa operação, no entanto, essa foi a menos complexa e com melhor performance com exceção da operação Vacuum DRY RUN em Scala**<br>
- **A primeira versão desenvolvida utilizei a leitura dos Delta Logs, Jsons e checkpoints, contudo, não consegui reproduzir exatamente a operação de Vacuum DRY RUN e a performance ficou pior devido a quantidade de validações que precisei adicionar**<br>
  - Nessa versão mapiei todos os arquivos marcados como Remove no log, embora, seja mais performático a precisão não era (dados não batiam) por alguns fatores, para contornar esses fatores o script ficou mais lento
  - Tentei reproduzir via Scala, contudo, meu conhecimento em Scala é limitado e ficou muito complexo
  - Falei com alguns engenheiros da comunidade Delta, mas não tive sucesso em evoluir via Scala
- **Se você rodar o Vaccum Dry Run via Scala ele printa o retorno, contudo, esse retorno vai para o Stdout e ficou muito complexo de recuperar**<br>
``%scala
vacuum tablename dry run``
- **Estou avaliando uma nova versão com delta.rs**
  - referencia: <https://github.com/delta-io/delta-rs/blob/main/python/docs/source/usage.rst>

Caso você queira se aventurar com Scala, aqui está o código-fonte:<br>
<https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>
<br>
Se você conseguir levantar essa quantidade de espaço de forma mais performática, me atualiza via comentário ou no Github.

**Ponto de atenção:** <br>
- **Para tabelas particionadas com muitas partições o tempo de execução pode ser mais demorado, por isso monitore as primeiras execuções com cautela, o uso é de sua responsabilidade, apesar de não ter nenhum risco mapeado até o momento, apenas pode gerar mais transações para sua storage**<br>
- **Custo das transações do Storage no Azure: Read operations (per 10,000) - R$0.0258 (Dois centavos por 10 mil operações) (Preço estimado em 21/04/2023)**

<br><h2>Descriçao dos parametros </h2>

### Parametros de controle

| Parametro | Valor | Descrição |
|-----------|-------|----------|
| `numThreadsParallel` | **15** | Número de threads paralelas, avalie o melhor valor para o seu ambiente, faça testes |
| `vacuumThreadsParallel` | **5** | Número de threads paralelas para execução do **Vacuum**, utilize um valor menor, pois, pode dar problema no cluster, avalie o melhor valor para o seu ambiente, faça testes |
| `runVacuum` | **False** | Se definido como **True** executa o Vacuum com os parâmetros configurados, o valor padrão é **False** execute a primeira vez no default para ter uma noção de como está o seu ambiente |
| `vacuumHours` | **168** | Quantidade de horas que será mantido de versões após o Vacuum, defina o melhor para o seu ambiente, o padrão é 7 dias |
| `vacuumThreshold` | **5x** | Executar o Vacuum apenas nas tabelas em que o storage for **5x** maior do que a versão atual, exemplo: **Uma tabela que tem 100GB de dados na versão atual e possuir 500GB no Storage, irá entrar na rotina de limpeza** |
| `enableLogs` | **False** | Se definido como **True** irá gerar logs para facilitar algumas análises como, por exemplo, o tempo de duração para cada tabela e o erro se houver, contudo,nível eleva bastante o tempo de processamento se você tiver muitas tabelas |
| `enableHistory` | **False** | Se definido como **True** mantém histórico de todas as execuções da rotina e cada execução possuirá um identificador único para poder relacionar entre as tabelas, se definido como **False** (valor padrão) as tabelas de logs (tableDetails e tableStorageFiles) sempre serão truncadas |

### Parametros para definição dos metadados

| Parametro | Valor | Descrição |
|-----------|-------|----------|
| `databaseTarget` | 'bronze*' | Define quais bancos de dados serão analisados, aceita regex com **, exemplo: todos os databases que começam com a palavra bronze: 'bronze*' |
| `tablesTarget` | '*' |  Definir quais tabelas serão analisadas, aceita regex com **, por padrão todas serão analisadas |
| `databaseCatalog` | 'db_controle' | Define em qual database será armazenado os logs, caso não exista será criado um novo |
| `tableCatalog` | 'tbCatalog' | Define nome da tabela de controle para armazenar as tabelas que serão analisadas, caso não exista a tabela será criada |
| `tbVacuumSummary` | 'tbVacuumSummary' | Define nome da tabela para armazenar o resultado agregado da execução, caso não exista a tabela será criada |
| `tableSizesMonitor` | 'tableSizesMonitor' | Define nome da tabela para armazenar o resultado agregado da execução com detalhes no nível de tabela, caso não exista a tabela será criada |
| `tableDetails` | 'bdaTablesDetails' | Define nome da tabela que irá armazenar o resultado do describe detail, caso não exista a tabela será criada |
| `tableStorageFiles` | 'bdaTablesStorageSize' | Define nome da tabela que irá armazenar o resultado do dbutils.fs.ls |
| `storageLocation` | 'abfss://[container]@[Storage].dfs.core.windows.net/pastaraiz/' [**Exemplo no Azure**]| Define endereço de storage principal, pode ser usado o valor absoluto ou um Mount (dbfs:/mnt/bronze/pastaRaiz/) |
| `tableCatalogLocation` | f'database=db_controle/table_name={tableCatalog}' | Define storage da tabela de catálogo |
| `tablesdetailsLocation` | f'database=db_controle/table_name={tableDetails}' | Define storage da tabela de detalhes do describe |
| `tableStorageFilesLocation` | f'database=db_controle/table_name={tableStorageFiles}' | Define storage da tabela de resultado do dbutils |
| `writeMode` | "overwrite" | Modo de escrita, "append" se `enableHistory` é verdadeiro, caso contrário "overwrite" |
| `identifier` | str(hash(datetime.today())) | Identificador unico para cada execução, usado para vincular as tabelas com suas devidas execuções |

## Objetos criados: 1 database e 5x tabelas

> 1x Database nomeado através da variáve databaseCatalog, por padrão o nome será **db_controle** <br>
> 1x Tabela de catalogo, irá armazenar a listagem, por padrão o nome será **tbCatalog**, se o parâmetro enableHistory estiver desabilitado ela será sobrescrita em cada execução <br>
> 1x Tabela para armazenar o resultado do describe detail, por padrão será chamada de **bdaTablesDetails**, se o parâmetro enableHistory estiver desabilitado ela será sobrescrita em cada execução <br>
> 1x Tabela para armazenar o resultado do List files, por padrão será chamada de **tableStorageFiles**, se o parâmetro enableHistory estiver desabilitado ela será sobrescrita em cada execução <br>
> 1x Tabela para armazenar o resultado agregado da execução com detalhes no nivel de tabela, por padrão será chamada de **tableSizesMonitor**, essa tabela nunca é truncada <br>
> 1x Tabela para armazenar o resultado agregado da execução, por padrão será chamada de **tbVacuumSummary**, essa tabela nunca é truncada <br>

## Monitoramento

> Monitore seu ambiente através das tabelas tbVacuumSummary e tableSizesMonitor<br>
> A tabela **tbVacuumSummary** armazena 1 linha por execução de dados sumarizados<br>
> A tabela **tableSizesMonitor** armazena 1 linha por tabela por execução com dados sumarizados<br>


## Benchmarch:

> Ambiente com mais de 3 mil tabelas - 200 TB de Storage - 12 horas - Cluster (1xnode DS5) - 50 Threads para analysis e 10 para vacuum - **Sem logs (enableLogs=False)** - Primeira execução<br>
> Ambiente com 300 tabelas - 5 TB de Storage - 1 hora - Cluster (1xnode DS3) - 25 Threads para analysis e 10 para vacuum - **Sem logs (enableLogs=False)** - Primeira execução<br>
> Ambiente com 300 tabelas - 5 TB de Storage - 2 horas - Cluster (1xnode DS3) - 25 Threads para analysis e 10 para vacuum - **Com logs (enableLogs=True)** - Primeira execução<br>
> Ambiente com 1000 tabelas - 10 GB de Storage - **6 horas - Cluster (1xnode DS3)** - 25 Threads para analysis e 10 para vacuum - **Com logs (enableLogs=True)** - Primeira execução<br>
Ambiente com 1000 tabelas - 10 GB de Storage - 6 horas - Cluster (1xnode DS3) - 25 Threads para analysis e 10 para vacuum - **Sem Logs (enableLogs=False)** - Primeira execução

## Cases reais:

> **Case 1 - Azure:** Liberado mais de 250 TB na primeira execução em um ambiente que não havia rotina<br>
> **Case 2 - GCP:** Liberado 5 TB de logs em um ambiente pequeno, onde o total de dados eram apenas 50 GB e o storage tinha 5 TB<br>
> **Case 3 - Azure:** Liberado em média 10 TB de logs por semana, utilizando em um Databricks Job com agendamento para todos os finais de semana<br>

## Implementações futuras:

> 1. Utilizar Unity Catalog<br>
> 2. Converter código para uma Lib em python<br>
> 3. Refatorar código usando a Lib e melhorando a usabilidade
> 4. Minimizar custos com dbutils.fs.ls, olhando direto para o transction log<br>

## Referencias:

<https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala>

<https://docs.databricks.com/sql/language-manual/delta-vacuum.html>

<br>

> ``Criado por: Reginaldo Silva``
  - [Blog Data In Action](https://datainaction.dev/)
  - [Github](https://github.com/reginaldosilva27)
