package dmp.util

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date

import dmp.common.Constant
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/7 17:38
  * @version 1.0
  */
object HbaseUtils {
  val logger:Logger = Logger.getLogger("hbase_logger")
  var connection:Connection = null
  //获取连接配置对象
  val configuration:Configuration = HBaseConfiguration.create()
  //设置连接hbase的参数
  configuration.set(Constant.HBASE_CONNECT_KEY,Constant.HBASE_CONNECT_VALUE)
  //获取Amin对象
  try{
    connection = ConnectionFactory.createConnection(configuration)
  } catch {
      case e:Exception => logger.warn("连接Hbase异常",e)
  }
  /**
    * 获取Admin对象
    */
  def getAdmin():Admin = {
    connection.getAdmin
  }
  def close(admin:Admin) = {
    if (admin != null) {
      admin.close
      admin.getConnection.close
    }
  }

  /**
    * spark写hbase
    */
  def writeHbase(rdd:RDD[(String, String)]) = {
    val table = connection.getTable(TableName.valueOf("user_tag"))
    rdd.map(x =>{
      val userid = x._1
      val tags = x._2
      val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val put:Put = new Put(Bytes.toBytes(userid))
      put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(tags))
      table.put(put)
    })
    //关闭连接
    table.close()
  }
}
