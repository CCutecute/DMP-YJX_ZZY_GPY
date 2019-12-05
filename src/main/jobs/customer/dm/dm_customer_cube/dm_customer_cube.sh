#!/bin/bash

#params
yesterday=`date -d "-1 day" +"%Y%m%d"`

#spark job
/home/framework/spark-2.2.3/bin/spark-sql \
--master yarn \
--name dm_customer_sources_code_job \
--S \
--hiveconf partitions=$partitions \
--hiveconf bdp_day=$yesterday \
--num-executors 10 \
--executor-memory 2G \
--executor-cores 2 \
-f dm_customer_cube.hql