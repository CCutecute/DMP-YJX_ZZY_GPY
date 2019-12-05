#!/bin/bash

#params
partitions=200
yesterday=`date -d "-1 day" +"%Y%m%d"`
release_status=01

 #spark sql job
/home/framework/spark-2.2.3/bin/spark-sql \
--master yarn  \
--name dw_release_customer_sql_job \
--S \
--hiveconf partitions=$partitions \
--hiveconf bdp_day=$yesterday \
--hiveconf release_status=$release_status \
--num-executors 10 \
--executor-memory 2G \
--executor-cores 2 \
-f dw_release_customer.hql