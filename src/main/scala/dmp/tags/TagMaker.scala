package dmp.tags

import dmp.common.Constant
import dmp.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.io.Source

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/7 11:26
  * @version 1.0
  */
object TagMaker {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession(false)
    val df: DataFrame = spark.read.parquet(Constant.LOG_SAVE_URL).persist()
    import org.apache.spark.sql.functions._

    //将 appName 文件 整为 Map
    val appNameMap = Source.fromFile(Constant.APP_DICT_URL).getLines.toList
      .map(str => (str.split("\t")(0), str.split("\t")(1)))
      .toMap

    //将 appName 文件 整为 广播变量
    val appNameBroadcast = spark.sparkContext.broadcast(appNameMap)

    val test: RDD[Map[String, Integer]] = df.rdd.map(row => {
      val adTypeTag: Map[String, Integer] = ADTypeTag.makeTag(row)
      val appNameTag = AppNameTag.makeTag(row, appNameBroadcast.value)

      val tags = adTypeTag ++ appNameTag
      tags
    })

    test.collect.toBuffer.foreach(println)
    spark.stop()
  }
}
