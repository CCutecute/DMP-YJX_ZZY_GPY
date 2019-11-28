--1 创建主题数据层数据库
create database dw_release;


--2 目标客户主题
create external table if not exists dw_release.dw_release_customer(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
  device_num string comment '设备唯一编码',
  device_type string comment '1 android| 2 ios | 9 其他',
  sources string comment '渠道',
  channels string comment '通道',
  idcard string comment '身份证',
  age int comment '年龄',
  gender string comment '性别',
  area_code string comment '地区',
  longitude string comment '经度',
  latitude string comment '纬度',
  matter_id string comment '物料代码',
  model_code string comment '模型代码',
  model_version string comment '模型版本',
  aid string comment '广告位id',
  ct bigint comment '创建时间'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dw/release_customer/'


--3 竞价主题
create external table if not exists dw_release.dw_release_bidding(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
  device_num string comment '设备唯一编码',
  device_type string comment '1 android| 2 ios | 9 其他',
  sources string comment '渠道',
  channels string comment '通道',
  bidding_type string comment '竞价类型 PMP直接出价|RTB竞价模式',
  bidding_price string comment '出价',
  cost_price string comment '成本',
  bidding_result string comment '竞价结果：1成功|0失败',
  aid string comment '广告位id',
  ct bigint comment '创建时间'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dw/release_bidding/'