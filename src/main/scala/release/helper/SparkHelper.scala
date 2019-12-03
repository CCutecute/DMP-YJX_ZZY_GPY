package release.helper

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/11/27 11:56
  * @version 1.0
  */
object SparkHelper {

  def createSpark(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate()
    //加载自定义函数
    //registerFun(spark)
    spark
  }


  def rangeDate(bdp_day_begin:String,bdp_day_end:String):Seq[String] = {
    val startData = new SimpleDateFormat("yyyy-MM-dd").parse(bdp_day_begin); //定义起始日期
    val endData = new SimpleDateFormat("yyyy-MM-dd").parse(bdp_day_end); //定义结束日期

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var buffer = new ListBuffer[String]
    buffer += dateFormat.format(startData.getTime())
    val tempStart = Calendar.getInstance()

    tempStart.setTime(startData)
    tempStart.add(Calendar.DAY_OF_YEAR, 1)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)
    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    if(bdp_day_begin != bdp_day_end)
    buffer += dateFormat.format(endData.getTime())
    buffer.toList
  }
}
