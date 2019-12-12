package dmp.util

import org.apache.spark.sql.Row

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/7 16:05
  * @version 1.0
  */
object TagUtils {
  def selectUserId(row:Row):String = {
    val imei = row.getAs[String]("imei")
    val mac = row.getAs[String]("mac")
    val idfa = row.getAs[String]("idfa")
    val openudid = row.getAs[String]("openudid")
    val androidid = row.getAs[String]("androidid")
    val imeimd5 = row.getAs[String]("imeimd5")
    val macmd5 = row.getAs[String]("macmd5")
    val idfamd5 = row.getAs[String]("idfamd5")
    val openudidmd5 = row.getAs[String]("openudidmd5")
    val androididmd5 = row.getAs[String]("androididmd5")
    val imeisha1 = row.getAs[String]("imeisha1")
    val macsha1 = row.getAs[String]("macsha1")
    val idfasha1 = row.getAs[String]("idfasha1")
    val openudidsha1 = row.getAs[String]("openudidsha1")
    val androididsha1 = row.getAs[String]("androididsha1")
    val list = List(imei,mac,idfa,openudid,androidid,imeimd5,macmd5,
      idfamd5,openudid,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1)
    for(userid <- list){
      if(userid.nonEmpty)
        return userid
    }
    return ""
  }
}
