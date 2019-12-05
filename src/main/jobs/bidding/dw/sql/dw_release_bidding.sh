#!/bin/bash

#params
partitions=200
yesterday=`date -d "-1 day" +"%Y%m%d"`
release_status=02
bidding_type_pmp=PMP
bidding_type_rtb=RTB
bidding_status_master=01
bidding_status_platform=02


 #spark sql job
/home/framework/spark-2.2.3/bin/spark-sql \
--master yarn  \
--name dw_release_bidding_sql_job \
--S \
--hiveconf partitions=$partitions \
--hiveconf bdp_day=$yesterday \
--hiveconf release_status=$release_status \
--hiveconf bidding_type_pmp=$bidding_type_pmp \
--hiveconf bidding_type_rtb=$bidding_type_rtb \
--hiveconf bidding_status_master=$bidding_status_master \
--hiveconf bidding_status_platform=$bidding_status_platform \
--num-executors 10 \
--executor-memory 2G \
--executor-cores 2 \
-f dw_release_bidding.hql