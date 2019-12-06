package dmp.util

import dmp.Entry.Log
import dmp.common.{Constant}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:
  * Copyright (c),2019,JingxuanYan
  * This program is protected by copyright laws.
  *
  * @author 闫敬轩
  * @date 2019/12/6 14:28
  * @version 1.0
  */

object SparkUtils {

  //封装对于各种Spark各种相关的组件进行获取


  def getSparkConf(): SparkConf = {

    val conf = new SparkConf().setAppName(Constant.APPLICATION_NAME).setMaster(Constant.MASTER)
    //conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    //指定序列化的类
    conf.registerKryoClasses(Array(classOf[Log]))
    conf.set("spark.serializer", Constant.SERIALIZER)
    conf

  }

  def getSparkSession(HiveSupport: Boolean): SparkSession = if (!HiveSupport)
    SparkSession.builder()
      .config(getSparkConf())
      .getOrCreate()
  else
    SparkSession.builder()
      .config(getSparkConf())
      .enableHiveSupport()
      .getOrCreate()

  def getSparkContext(): SparkContext = {
    System.getProperty("HADOOP_USER_NAME", "root")
    SparkSession.builder()
      .appName(Constant.APPLICATION_NAME)
      .master(Constant.MASTER)
      .enableHiveSupport()
      .getOrCreate()
      .sparkContext

  }

  def main(args: Array[String]): Unit = {
    println(getSparkSession(true))
  }

}
