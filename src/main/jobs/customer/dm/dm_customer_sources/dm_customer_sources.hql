set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000
set hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table dm_release.dm_customer_sources partition(bdp_day='${bdp_day}')
select 
sources,
channels,
device_type,
count(distinct(device_num)) user_count,
count(distinct(device_num)) total_count
from
dw_release.dw_release_customer
where bdp_day='${bdp_day}'
group by
sources,
channels,
device_type
;