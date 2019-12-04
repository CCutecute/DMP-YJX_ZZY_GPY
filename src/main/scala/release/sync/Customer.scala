package com.ad


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode}


/**
  * Description:
  * Copyright (c),2019,JingxuanYan
  * This program is protected by copyright laws.
  *
  * @author 闫敬轩
  * @date 2019/11/26 8:45
  * @version 1.0
  */
object Customer {

  import org.apache.spark.sql.SparkSession

  def handleReleaseJobs(bdp_day_begin:String,bdp_day_end:String):Unit = {
    val sconf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.shuffle.partitions", "32")
      .set("hive.merge.mapfiles", "true")
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
      .set("spark.sql.crossJoin.enabled", "true")
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")

    val spark:SparkSession = SparkHelper.createSpark(sconf)

    val bdp_days: Seq[String] = SparkHelper.rangeDate(bdp_day_begin,bdp_day_end)
    for(bdp_day <- bdp_days){
      println(bdp_day)
      handleJobs(spark,bdp_day)
    }
    //    createTable1(spark)

    spark.stop()
  }

  def handleJobs(spark:SparkSession,bdp_day:String):Unit = {
    val df: DataFrame = spark.read.parquet("hdfs://10.0.88.246:9000/data/release/ods/release_session/")
    handleCustomerJobs(spark,df,bdp_day)
    //    handleExposureJobs(spark,df,bdp_day)
  }

  def handleCustomerJobs(spark:SparkSession,df:DataFrame,bdp_day:String):Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //目标客户dw层
    val customerDWCondition = $"dt" === s"${bdp_day}" and $"release_status" === "01"
    val customerODSColumns = ReleaseColumnsHelper.selectODSReleaseCustomerColumns(spark)
    val customerDWDF = df
      .select(customerODSColumns:_*)
      .withColumn("dt", from_unixtime(substring(col("ct"), 0, 10), "yyyy-MM-dd"))
      .where(customerDWCondition)
      .cache()
    //    customerDWDF.show()
    //    customerDWDF.write.format("json").mode(SaveMode.Overwrite).save(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/customerDWDF/bdp_day=${bdp_day}")
    customerDWDF.write.mode(SaveMode.Append).partitionBy("dt").saveAsTable("dw_release.dw_release_customer")
    println(s"${bdp_day}目标客户dw层√")

    //    目标客户dm层
    val customerDMCondition = $"dt" === s"${bdp_day}"
    val customerDWColumns = ReleaseColumnsHelper.selectDWReleaseCustomerColumns(spark)
    val customerDF = customerDWDF
      .select(customerDWColumns:_*)
      .where(customerDMCondition)
      .cache()
    val customerGroupColumns:Seq[Column] = Seq($"${ReleaseConstant.COL_RELEASE_DT}",$"${ReleaseConstant.COL_RELEASE_SOURCES}")
    val customerDMColumns = ReleaseColumnsHelper.selectDMCustomerSourcesColumns(spark)
    val customerDMDF: DataFrame = customerDF
      .groupBy(customerGroupColumns:_*)
      .agg(countDistinct("device_num").as("user_count"),
        count("device_num").as("total_count")
      )
      .select(customerDMColumns:_*)
      .repartition(1)
    //    customerDMDF.show()
    //    customerDMDF.write.format("json").mode(SaveMode.Overwrite).save(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/customerDMDF/bdp_day=${bdp_day}")
    customerDMDF.write.mode(SaveMode.Append).partitionBy("dt").saveAsTable("dm_release.dm_release_customer")
    println(s"${bdp_day}目标客户dm层√")
  }

  def handleExposureJobs(spark:SparkSession,df:DataFrame,bdp_day:String):Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //曝光dw层
    val exposureDWCondition = $"dt" === s"${bdp_day}" and $"release_status" === "03"
    val exposureDWDF = df
      .select($"release_session", $"device_num", $"sources",$"ct")
      .withColumn("dt", from_unixtime(substring(col("ct"), 0, 10), "yyyy-MM-dd"))
      .where(exposureDWCondition)
      .repartition(1)
      .cache()
    //    exposureDWDF.show()
    //    exposureDWDF.write.format("json").mode(SaveMode.Overwrite).save(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/exposureDWDF/bdp_day=${bdp_day}")
    //    exposureDWDF.write.mode(SaveMode.Append).saveAsTable("dw_release.dw_release_exposure")
    println(s"${bdp_day}曝光dw层√")

    //曝光dm层
    val exposureDMCondition = $"dt" === s"${bdp_day}"
    val exposureDF = exposureDWDF
      .select("release_session", "device_num", "dt", "sources")
      .where(exposureDMCondition)
      .cache()
    val exposureDMDF: DataFrame = exposureDF
      .groupBy("dt", "sources")
      .agg(count("device_num").as("exposure_num"))
      .repartition(1)
    //customerDMDF.show()
    //    exposureDMDF.write.format("json").mode(SaveMode.Overwrite).save(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/exposureDMDF/bdp_day=${bdp_day}")
    //    exposureDMDF.write.mode(SaveMode.Append).saveAsTable("dm_release.dm_release_exposure")
    println(s"${bdp_day}曝光dm层√")
  }

  def createTable1(spark:SparkSession):Unit = {
    //    val customerDMDF: DataFrame = spark.read.json(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/customerDMDF/")
    //    val exposureDMDF: DataFrame = spark.read.json(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/exposureDMDF/")
    val customerDMDF: DataFrame = spark.read.table("dm_release.dm_release_customer")
    val exposureDMDF: DataFrame = spark.read.table("dm_release.dm_release_exposure")


    import spark.implicits._
    import org.apache.spark.sql.functions._
    customerDMDF
      .join(exposureDMDF,customerDMDF("dt") === exposureDMDF("dt") and customerDMDF("sources") === exposureDMDF("sources"))
      .select(customerDMDF("dt"),customerDMDF("sources"),$"total_count",$"exposure_num")
      .show(50)
  }

  def main(args: Array[String]): Unit = {

    val bdp_day_begin = "2019-11-25"
    val bdp_day_end = "2019-11-30"
    handleReleaseJobs(bdp_day_begin,bdp_day_end)
  }


}
