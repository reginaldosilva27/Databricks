##Tabela de migração: Estrategias por tipo de tabela

| Id | Tipo HMS | Location       | Tipo UC            | Método                  |
|----|----------|----------------|--------------------|--------------------------|
| 1  | Managed  | DBFS Root      | Managed/External   | CTAS / DEEP CLONE        |
| 2  | Managed  | DBFS Root      | Managed/External   | CTAS / DEEP CLONE        |
| 3  | Hive SerDe  | DBFS Root      | Managed/External   | CTAS / DEEP CLONE        |
| 4  | Managed  | Mount          | External           | SYNC com Convert         |
| 5  | Managed  | Mount          | Managed            | CTAS / DEEP CLONE        |
| 6  | External | Mount          | External           | SYNC                     |
| 7  | External | Mount          | Managed            | CTAS / DEEP CLONE        |
| 8  | Managed  | Cloud Storage  | External           | SYNC com Convert         |
| 9  | Managed  | Cloud Storage  | Managed            | CTAS / DEEP CLONE        |
| 10  | External | Cloud Storage  | External           | SYNC                     |
| 11  | External | Cloud Storage  | Managed            | CTAS / DEEP CLONE        |

## Observação importante
- **set spark.databricks.sync.command.enableManagedTable=true;**
  - Ao usar essa opção, você não pode dropar a tabela no HMS, pois, o dados serão excluídos do Storage
  - Caso queira dropar, use o script Scala para trocar ela de Managed para External

## Tabelas Managed vs External

- **Tabelas Managed**:
  - Dados e metadados são gerenciados pelo Unity Catalog.
  - Os dados são armazenados no local especificado pelo catálogo Unity (tipicamente em armazenamento cloud).
  - A exclusão de uma tabela managed remove também os dados.
    - Se for HMS os dados são removidos imediatamente
    - Se for no UC os dados são mantidos por mais 30 dias
      - Aqui voce pode usar o UNDROP até 7 dias

- **Tabelas External**:
  - Apenas os metadados são gerenciados pelo Unity Catalog, os dados permanecem no armazenamento externo (geralmente em um bucket ou outro recurso cloud).
  - A exclusão de uma tabela external remove apenas os metadados; os dados permanecem no armazenamento original.
  - Permite que os dados sejam compartilhados entre diferentes sistemas ou aplicações.

### DBFS Root vs Mount vs Cloud Storage

- **DBFS Root**:
  - O sistema de arquivos distribuído do Databricks (Databricks File System).
  - Armazenamento temporário e volátil, com possíveis limitações em operações de longa duração.
  - Os dados ficam fisicamente no storage da Databricks que voce não tem acesso

- **Mount**:
  - Uma forma de acessar o armazenamento externo (como S3, ADLS) no DBFS como se fosse um diretório local.
  - Os dados permanecem no armazenamento externo, mas podem ser acessados dentro de Databricks via caminhos montados.

- **Cloud Storage**:
  - Armazenamento na nuvem (ex: AWS S3, Azure Data Lake, Google Cloud Storage) onde os dados podem ser armazenados e acessados diretamente.
  - Mais flexível para armazenamento de grande volume e soluções a longo prazo.

### Métodos CTAS, DEEP CLONE e SYNC

- **CTAS (Create Table As Select)**:
  - Método usado para criar uma nova tabela a partir dos resultados de uma consulta SQL.
  - A nova tabela pode ser criada com dados agregados ou filtrados.
  - Exemplo de uso: `CREATE TABLE nova_tabela AS SELECT * FROM tabela_existente WHERE condição`.

- **DEEP CLONE**:
  - Método utilizado para clonar tabelas, incluindo seus dados, metadados e histórico de transações.
  - Utilizado para cópia rápida de tabelas, útil em cenários de backup ou migração.
  - Exemplo: `DEEP CLONE origem DESTINO` cria uma cópia completa da tabela de origem.

- **SYNC**:
  - Sincroniza tabelas external com o Unity Catalog, garantindo que o catálogo reflita as alterações feitas diretamente no armazenamento.
  - Essencial para manter a consistência entre os metadados no Unity Catalog e o armazenamento externo.
  - Útil para cenários onde os dados podem ser alterados por fora do Databricks.


Post Databricks:
https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore#appendix

Notebook oficial:
  https://notebooks.databricks.com/notebooks/uc-upgrade-scenario-with-examples-for-blog.dbc?_gl=1*1nrxwtq*_gcl_au*OTUxMzE5NDg3LjE2OTM0NjcxNDM.
