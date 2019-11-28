--1 创建主题数据层数据库
create database dm_release;


--2 目标客户集市

--1 渠道用户统计
create external table if not exists dm_release.dm_customer_sources(
  sources string comment '渠道',
  channels string comment '通道',
  device_type string comment '1 android| 2 ios | 9 其他',
  user_count bigint comment '目标客户数量',
  total_count bigint comment '目标客户投放总量'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dm/release_customer/dm_customer_sources/'


--2 目标客户多维统计
create external table if not exists dm_release.dm_customer_cube(
  sources string comment '渠道',
  channels string comment '通道',
  device_type string comment '1 android| 2 ios | 9 其他',
  age_range string comment '1:18岁以下|2:18-25岁|3 26-35岁|4 36-45岁|5 45岁以上',
  gender string comment '性别',
  area_code string comment '地区',
  user_count bigint comment '目标客户数量',
  total_count bigint comment '目标客户投放总量'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dm/release_customer/dm_customer_cube/'
