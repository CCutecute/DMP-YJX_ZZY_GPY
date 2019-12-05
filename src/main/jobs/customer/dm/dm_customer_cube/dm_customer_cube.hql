set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000
set hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table dm_release.dm_customer_cube partition(bdp_day='${bdp_day}')
select
sources,
channels,
device_type,
gender,
area_code,
(case when age<18 then '1'
when age between 18 and 25 then '2'
when age between 26 and 35 then '3'
when age between 36 and 45 then '4'
else '5'
end) age_range,
count(distinct(device_num)) user_count,
count(distinct(device_num)) total_count
from
dw_release.dw_release_customer
where bdp_day='${bdp_day}'
group by
sources,
channels,
device_type,
gender,
area_code,
(case when age<18 then '1'
when age between 18 and 25 then '2'
when age between 26 and 35 then '3'
when age between 36 and 45 then '4'
else '5'
end)
;
