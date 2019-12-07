package dmp.tags

import dmp.common.Constant
import dmp.util.{SparkUtils, TagUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
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
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //将 appName 文件 整为 Map
    val appNameMap = Source.fromFile(Constant.APP_DICT_URL).getLines.toList
      .map(str => {
        val splited: Array[String] = str.split("\t")
        val appid = splited(0)
        val appName = splited(1)
        (appid,appName)
      })
      .toMap

    //将 appName 文件 整为 广播变量
    val appNameBroadcast = spark.sparkContext.broadcast(appNameMap)

    //将stopword文件整为Map
    val stopwordsMap = Source.fromFile(Constant.STOPWORDS_URL).getLines.toList
      .map(str => (str,"")).toMap

    //将stopword文件整为广播变量
    val stopwordsBroadcast = spark.sparkContext.broadcast(stopwordsMap)

    val user_tag: RDD[(String, Map[String, Integer])] = df.rdd
      .filter(row => TagUtils.selectUserId(row).nonEmpty)
      .map(row => {
      val adTypeTag: Map[String, Integer] = ADTypeTag.makeTag(row)
      val appNameTag = AppNameTag.makeTag(row, appNameBroadcast.value)
      val channelTag = ChannelTag.makeTag(row)
      val deviceTag = DeviceTag.makeTag(row)
      val keyWordTag = KeyWordTag.makeTag(row, stopwordsBroadcast.value)
      val regionTag = RegionTag.makeTag(row)
      val tags = adTypeTag ++ appNameTag ++ channelTag ++ deviceTag ++ keyWordTag ++ regionTag
      val userid = TagUtils.selectUserId(row)
      (userid, tags)
    })
    //    test.collect.toBuffer.foreach(println)
    //      test.toDF.show(1000,false)
    val user_tag_agg: RDD[(String, mutable.Map[String, Integer])] = user_tag
      .groupBy(_._1)
      .map(info => {
        val userid = info._1
        val tagsIte = info._2
        var map = scala.collection.mutable.Map[String, Integer]()
        for (tags <- tagsIte) {
          val tagsMap = tags._2
          for (tag <- tagsMap) {
            if (map.contains(tag._1))
              map(tag._1) = map(tag._1) + tag._2
            else
              map += tag
          }
        }
        (userid, map)
      })

    user_tag_agg.collect.toBuffer.foreach(println)

    //写入hbase中



    spark.stop()
  }
}
