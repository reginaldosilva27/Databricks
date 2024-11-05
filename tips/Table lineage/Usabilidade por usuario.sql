-- tabelas mais lidas por dia
select
  loginName,
  catalogName,
  schemaName,
  tableName,
  sum(READS) as READS,
  sum(WRITES) as WRITES
from
  (
    select
      read.created_By as loginName,
      t.table_catalog as catalogName,
      t.table_schema as schemaName,
      t.table_name as tableName,
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
      )
    group by
      all
    union all
    select
     write.created_By as loginName,
      t.table_catalog as catalogName,
      t.table_schema as schemaName,
      t.table_name as tableName,
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
      )
    group by
      all
  ) Tabs
group by
  all
order by
  1 desc
