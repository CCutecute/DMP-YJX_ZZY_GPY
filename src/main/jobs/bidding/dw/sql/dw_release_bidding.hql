set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;

with bidding as (
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    get_json_object(exts,'$.bidding_status') as bidding_status,
    get_json_object(exts,'$.bidding_type') as bidding_type,
    get_json_object(exts,'$.bidding_price') as bidding_price,
    get_json_object(exts,'$.aid') as aid,
    ct,
    bdp_day
from ods_release.ods_01_release_session
where
    bdp_day = '${bdp_day}'
and
    release_status = '${release_status}'
),
pmp as (
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    bidding_type,
    bidding_price,
    bidding_price as cost_price,
    "1" as bidding_result,
    aid,
    ct,
    bdp_day
from bidding
where
    bidding_type = 'PMP'
),
rtb as (
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    bidding_status,
    bidding_type,
    bidding_price,
    aid,
    ct,
    bdp_day
from bidding
where
    bidding_type = 'RTB'
),
rtb_master as (
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    bidding_status,
    bidding_type,
    bidding_price,
    aid,
    ct,
    bdp_day
from rtb
where
    bidding_status = '01'
),
rtb_platform as (
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    bidding_status,
    bidding_type,
    bidding_price,
    aid,
    ct,
    bdp_day
from rtb
where
    bidding_status = '02'
),
rtb_merge as (
select
    m.release_session,
    m.release_status,
    m.device_num,
    m.device_type,
    m.sources,
    m.channels,
    m.bidding_type,
    m.bidding_price,
    if(isnotnull(p.bidding_price),p.bidding_price, '0') as cost_price,
    if(isnotnull(p.bidding_price),'1','0') as bidding_result,
    m.aid,
    m.ct,
    m.bdp_day
from rtb_master m left join  rtb_platform p
on
    m.release_session = p.release_session
),
bidding_total as (
    select * from pmp
    union
    select * from rtb_merge
)

insert overwrite table dw_release.dw_release_bidding partition(bdp_day)
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    bidding_type,
    bidding_price,
    cost_price,
    bidding_result,
    aid,
    ct,
    bdp_day
from bidding_total
