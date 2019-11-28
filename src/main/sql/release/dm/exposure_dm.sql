--1 创建主题数据层数据库
create database if not exists dm_release;


--2 曝光主题集市

--1 渠道曝光统计
create external table if not exists dm_release.dm_exposure_sources(
  sources string comment '渠道',
  channels string comment '通道',
  device_type string comment '1 android| 2 ios | 9 其他',
  exposure_count bigint comment '曝光量',
  exposure_rates double comment '曝光率'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dm/release_exposure/dm_exposure_sources/'


--2 曝光多维统计
create external table if not exists dm_release.dm_exposure_cube(
  sources string comment '渠道',
  channels string comment '通道',
  device_type string comment '1 android| 2 ios | 9 其他',
  age_range string comment '1:18岁以下|2:18-25岁|3 26-35岁|4 36-45岁|5 45岁以上',
  gender string comment '性别',
  area_code string comment '地区',
  exposure_count bigint comment '曝光量',
  exposure_rates double comment '曝光率'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dm/release_exposure/dm_exposure_cube/'
