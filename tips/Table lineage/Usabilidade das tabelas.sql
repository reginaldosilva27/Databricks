-- tabelas mais lidas
-- Aplique filtros de datas se achar necessário
-- Customize conforme sua necessidade
select
  catalogName,
  schemaName,
  tableName,
  min(first_read) as first_read,
  max(last_read) as last_read,
  min(first_write) as first_write,
  max(last_write) as last_write,
  sum(READS) as READS,
  sum(WRITES) as WRITES
from
  (
    select
      t.table_catalog as catalogName,
      t.table_schema as schemaName,
      t.table_name as tableName,
      MIN(read.event_date) first_read,
      MAX(read.event_date) last_read,
      null first_write,
      null last_write,
      sum(
        case
          when read.source_table_name is not null then 1
          else 0
        end
      ) READS,
      0 WRITES
    from
      system.information_schema.tables t
      left join system.access.table_lineage read on t.table_name = read.source_table_name
      and t.table_schema = read.source_table_schema
      and t.table_catalog = read.source_table_catalog
    where
      t.table_catalog not in('system')
      and t.table_schema not in('information_schema')
      and (
        read.target_type in('TABLE')
        or read.target_type is null
      )
    group by
      all
    union all
    select
      t.table_catalog as catalogName,
      t.table_schema as schemaName,
      t.table_name as tableName,
      null first_read,
      null last_read,
      MIN(write.event_date) first_write,
      MAX(write.event_date) last_write,
      0 READS,
      sum(
        case
          when write.target_table_name is not null then 1
          else 0
        end
      ) WRITES
    from
      system.information_schema.tables t
      left join system.access.table_lineage write on t.table_name = write.target_table_name
      and t.table_schema = write.target_table_schema
      and t.table_catalog = write.target_table_catalog
    where
      t.table_catalog not in('system')
      and t.table_schema not in('information_schema')
      and (
        write.target_type in('TABLE')
        or write.target_type is null
      )
    group by
      all
  ) Tabs
group by
  all
order by
  READS DESC,
  WRITES DESC
