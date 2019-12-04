package com.ad

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Description:
  * Copyright (c),2019,JingxuanYan
  * This program is protected by copyright laws.
  *
  * @author 闫敬轩
  * @date 2019/11/26 18:50
  * @version 1.0
  */
object Bidding {

  import org.apache.spark.sql.SparkSession

  def handleReleaseJobs(bdp_day_begin:String,bdp_day_end:String):Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val bdp_days: Seq[String] = SparkHelper.rangeDate(bdp_day_begin,bdp_day_end)
    for(bdp_day <- bdp_days){
      println(bdp_day)
      handleJobs(spark,bdp_day)
    }
    createTable2(spark)
    spark.stop()
  }

  def handleJobs(spark:SparkSession,bdp_day:String):Unit = {
    val df: DataFrame = spark.read.parquet("hdfs://10.0.88.246:9000/data/release/ods/release_session/")
    handleBiddingJobs(spark,df,bdp_day)
  }

  def handleBiddingJobs(spark:SparkSession,df:DataFrame,bdp_day:String):Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //目标客户dw层
    //$"bidding_price",$"cost_price",$"bidding_result"
    val biddingDWCondition = $"dt" === s"${bdp_day}" and $"release_status" === "02"
    val biddingDF = df
      .select($"release_session",
        $"device_num",
        $"sources",
        $"ct",
        get_json_object(col("exts"),"$.bidding_type").as("bidding_type"),
        get_json_object(col("exts"),"$.bidding_price").as("bidding_price"),
        get_json_object(col("exts"),"$.bidding_status").as("bidding_status"))
      .withColumn("dt", from_unixtime(substring(col("ct"), 0, 10), "yyyy-MM-dd"))
      .where(biddingDWCondition)
      .cache()

    //    biddingDF.show()
    val pmpDF = biddingDF
      .select($"release_session",
        $"device_num",
        $"sources",
        $"bidding_type",
        $"bidding_price",
        $"bidding_price".as("cost_price"),
        lit("1").as("bidding_result"),
        $"ct",
        $"dt")
      .where($"bidding_type" === "PMP")
      .cache()
    val RTBMasterDF = biddingDF
      .select($"release_session",
        $"device_num",
        $"sources",
        $"bidding_status",
        $"bidding_type",
        $"bidding_price",
        $"ct",
        $"dt")
      .where($"bidding_type" === "RTB" and $"bidding_status" === "01")
    val RTBPlatformDF = biddingDF
      .select($"release_session",
        $"device_num",
        $"sources",
        $"bidding_status".as("bidding_status"),
        $"bidding_type",
        $"bidding_price".as("bidding_price"),
        $"ct",
        $"dt")
      .where($"bidding_type" === "RTB" and $"bidding_status" === "02")
    //    RTBPlatformDF.show()
    val rtbDF = RTBMasterDF.join(RTBPlatformDF,RTBMasterDF("release_session") === RTBPlatformDF("release_session"),"left")
      .select(
        RTBMasterDF("release_session"),
        RTBMasterDF("device_num"),
        RTBMasterDF("sources"),
        RTBMasterDF("bidding_type"),
        RTBMasterDF("bidding_price"),
        //        RTBMasterDF("bidding_status"),
        //        RTBPlatformDF("bidding_status"),
        when(RTBPlatformDF("bidding_status") isNotNull,RTBPlatformDF("bidding_price")).otherwise("0").as("cost_price"),
        when(RTBPlatformDF("bidding_status") isNotNull,"1").otherwise("0").as("bidding_result"),
        RTBMasterDF("ct"),
        RTBMasterDF("dt")
      )
    //        .show()
    val biddingDWDF = (pmpDF union rtbDF).repartition(1).cache()
    //    biddingDWDF.write.format("json").mode(SaveMode.Overwrite).save(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/biddingDWDF/bdp_day=${bdp_day}")
    biddingDWDF.write.mode(SaveMode.Append).saveAsTable("dw_release.dw_release_bidding")
    println("竞价dw层√")

    //竞价dm层
    val biddingDMCondition = $"dt" === s"${bdp_day}"
    val biddingtmpDF = biddingDWDF
      .select(
        $"release_session",
        $"device_num",
        $"sources",
        $"bidding_type",
        $"bidding_price",
        $"cost_price",
        $"bidding_result",
        $"ct",
        $"dt"
      )
      .where(biddingDMCondition)
      .cache()
    val biddingDMDF = biddingtmpDF.groupBy("dt","sources")
      .agg((sum(when($"bidding_result" === "1","1").otherwise("0"))/count("device_num")).as("rate"),
        sum("bidding_price").as("bidding_price"),
        sum("cost_price").as("cost_price"))
      .repartition(1)
    //    biddingDMDF.write.format("json").mode(SaveMode.Overwrite).save(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/biddingDMDF/bdp_day=${bdp_day}")
    biddingDMDF.write.mode(SaveMode.Append).saveAsTable("dm_release.dm_release_bidding")
    println("竞价dm层√")
    //    biddingDMDF.show()
  }

  def createTable2(spark:SparkSession):Unit = {
    //    val customerDMDF: DataFrame = spark.read.json(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/customerDMDF/")
    //    val exposureDMDF: DataFrame = spark.read.json(s"file:///D://XSQ-BigData-24/项目/广告投放项目/test/exposureDMDF/")
    val biddingDMDF: DataFrame = spark.read.table("dm_release.dm_release_bidding")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    biddingDMDF.show(50)

  }


  def main(args: Array[String]): Unit = {

    val bdp_day_begin = "2019-11-25"
    val bdp_day_end = "2019-11-30"
    handleReleaseJobs(bdp_day_begin,bdp_day_end)
  }
}
