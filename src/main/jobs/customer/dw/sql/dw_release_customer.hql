set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;

with release_customer as (
select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    get_json_object(exts,'$.idcard') as idcard,
    ( cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(exts,'$.idcard'), '(\\d{6})(\\d{4})(\\d{4})', 2) as int) ) as age,
    cast(regexp_extract(get_json_object(exts,'$.idcard'), '(\\d{6})(\\d{8})(\\d{4})', 3) as int) % 2 as gender,
    get_json_object(exts,'$.area_code') as area_code,
    get_json_object(exts,'$.longitude') as longitude,
    get_json_object(exts,'$.latitude') as latitude,
    get_json_object(exts,'$.matter_id') as matter_id,
    get_json_object(exts,'$.model_code') as model_code,
    get_json_object(exts,'$.model_version') as model_version,
    get_json_object(exts,'$.aid') as aid,
    ct,
    bdp_day
from ods_release.ods_01_release_session
where
    bdp_day = '${bdp_day}'
and
    release_status ='${release_status}'
 )

 insert into table dw_release.dw_release_customer partition(bdp_day)
 select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    idcard,
    age,
    gender,
    area_code,
    longitude,
    latitude,
    matter_id,
    model_code,
    model_version,
    aid,
    ct,
    bdp_day
 from release_customer