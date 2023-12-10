-- Todas as tabelas do seu ambiente
select * from system.information_schema.tables where table_owner <> 'System user';

-- Todas as colunas de cada tabela
select c.table_name,array_join(collect_set(column_name), ',') as columns from system.information_schema.columns c
inner join system.information_schema.tables t on c.table_name = t.table_name and c.table_catalog = t.table_catalog
where t.table_owner <> 'System user'
group by all;

-- Quantidade de tabelas por schema e catalog
select table_catalog,table_schema,count(*) as qtdTables
from system.information_schema.tables where table_owner <> 'System user'
group by all;

-- Auditoria do seu ambiente
select * from system.access.audit order by event_time desc;

-- Ultimo acesso nas suas tabelas
select LastAccess.event_time,LastAccess.entity_type,LastAccess.created_by,* from system.information_schema.tables a
LEFT JOIN 
LATERAL (select max(b.event_time) as event_time, LAST(b.entity_type) as entity_type, LAST(b.created_by) as created_by
from system.access.table_lineage b where b.target_table_name = a.table_name) as LastAccess
where a.table_owner <> 'System user';

-- Quem acessou sua tabela e quando?
select * from system.access.table_lineage where target_table_name = 'tbordersliquid'
order by event_time desc;

-- Todos os clusters do ambiente
select cluster_source,count(*) as qtd from system.compute.clusters
group by all;

-- Clusters All Purpose
select * from system.compute.clusters where cluster_source = 'UI';

-- Job Clusters mais custosos
SELECT usage_metadata.job_id as `Job ID`, sum(usage_quantity) as `DBUs`
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL
GROUP BY `Job ID`
ORDER BY `DBUs` DESC;

-- Cluster mais custoso
select b.cluster_name, sum(usage_quantity) as `DBUs Consumed` from system.billing.usage a 
inner join system.compute.clusters b on a.usage_metadata.cluster_id = b.cluster_id
where usage_metadata.cluster_id is not null
group by all
order by 2 desc;

-- Cluster All purpose mais custoso
select usage_date as `Date`, sum(usage_quantity) as `DBUs Consumed` from system.billing.usage a 
inner join system.compute.clusters b on a.usage_metadata.cluster_id = b.cluster_id
where usage_metadata.cluster_id is not null
group by all
order by 1 desc;


-- Cluster mais custoso em USD
select b.cluster_name, sum(usage_quantity) as `DBUs Consumed`, (sum(usage_quantity) * max(c.pricing.default)) as TotalUSD 
from system.billing.usage a 
inner join system.compute.clusters b on a.usage_metadata.cluster_id = b.cluster_id
inner join system.billing.list_prices c on c.sku_name = a.sku_name
where usage_metadata.cluster_id is not null
and usage_start_time between '2023-11-01' and '2023-11-30'
group by all
order by 3 desc;


-- total em USD por mês
select month(usage_end_time) as mes,sum(usage_quantity) as `DBUs Consumed`, (sum(usage_quantity) * max(c.pricing.default)) as TotalUSD 
from system.billing.usage a 
inner join system.billing.list_prices c on c.sku_name = a.sku_name
group by all
order by 1 desc

-- Execuções do PREDICTIVE OPTIMIZATION
select * from  system.storage.predictive_optimization_operations_history;
