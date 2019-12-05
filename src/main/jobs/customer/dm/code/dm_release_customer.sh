#!/bin/bash

#params
yesterday=`date -d "-1 day" +"%Y%m%d"`

 #spark job
/home/framework/spark-2.2.3/bin/spark-submit \
--class com.qf.bigdata.release.etl.release.dm.DMReleaseCustomer \
--master yarn \
--deploy-mode client \
--num-executors 10 \
--executor-memory 2G \
--executor-cores 2 \
 release-1.0-SNAPSHOT-DMReleaseCustomerJob.jar \
 dm_release_customer_code_job $yesterday $yesterday