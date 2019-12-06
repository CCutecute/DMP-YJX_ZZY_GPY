package dmp.common

import org.apache.spark.sql.SaveMode

/**
  * Description:
  * Copyright (c),2019,JingxuanYan
  * This program is protected by copyright laws.
  *
  * @author 闫敬轩
  * @date 2019/12/6 14:28
  * @version 1.0
  */

object Constant {

  val APPLICATION_NAME ="DMP"
  val MASTER ="local[2]"

  val LOG_URL = "D://XSQ-BigData-24/项目/DMP项目资料/2016-10-01_06_p1_invalid.1475274123982.log"
  val LOG_SAVE_URL = "D://XSQ-BigData-24/项目/DMP项目资料/2016-10-01_06_p1_invalid.1475274123982.log.parquet"

  val DB_URL ="jdbc:mysql://127.0.0.1:3306/advertising?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
  val DB_USERNAME="root"
  val DB_PASSWORD="123321"

  val SERIALIZER ="org.apache.spark.serializer.KryoSerializer"
  val SAVE_MODE = SaveMode.Overwrite
}

/**
  *
  *
  * 样例类 用于解析日志中的字段
  */

//case class Log(
//                 sessionid: String,
//                 advertisersid: Int,
//                 adorderid: Int,
//                 adcreativeid: Int,
//                 adplatformproviderid: Int,
//                 sdkversion: String,
//                 adplatformkey: String,
//                 putinmodeltype: Int,
//                 requestmode: Int,
//                 adprice: Double,
//                 adppprice: Double,
//                 requestdate: String,
//                 ip: String,
//                 appid: String,
//                 appname: String,
//                 uuid: String,
//                 device: String,
//                 client: Int,
//                 osversion: String,
//                 density: String,
//                 pw: Int,
//                 ph: Int,
//                 long_s: String,
//                 lat: String,
//                 provincename: String,
//                 cityname: String,
//                 ispid: Int,
//                 ispname: String,
//                 networkmannerid: Int,
//                 networkmannername: String,
//                 iseffective: Int,
//                 isbilling: Int,
//                 adspacetype: Int,
//                 adspacetypename: String,
//                 devicetype: Int,
//                 processnode: Int,
//                 apptype: Int,
//                 district: String,
//                 paymode: Int,
//                 isbid: Int,
//                 bidprice: Double,
//                 winprice: Double,
//                 iswin: Int,
//                 cur: String,
//                 rate: Double,
//                 cnywinprice: Double,
//                 imei: String,
//                 mac: String,
//                 idfa: String,
//                 openudid: String,
//                 androidid: String,
//                 rtbprovince: String,
//                 rtbcity: String,
//                 rtbdistrict: String,
//                 rtbstreet: String,
//                 storeurl: String,
//                 realip: String,
//                 isqualityapp: Int,
//                 bidfloor: Double,
//                 aw: Int,
//                 ah: Int,
//                 imeimd5: String,
//                 macmd5: String,
//                 idfamd5: String,
//                 openudidmd5: String,
//                 androididmd5: String,
//                 imeisha1: String,
//                 macsha1: String,
//                 idfasha1: String,
//                 openudidsha1: String,
//                 androididsha1: String,
//                 uuidunknow: String,
//                 userid: String,
//                 iptype: Int,
//                 initbidprice: Double,
//                 adpayment: Double,
//                 agentrate: Double,
//                 lomarkrate: Double,
//                 adxrate: Double,
//                 title: String,
//                 keywords: String,
//                 tagid: String,
//                 callbackdate: String,
//                 channelid: String,
//                 mediatype: Int
//               )
//
//class LogBakLog(
//                 sessionid: String,
//                 advertisersid: Int,
//                 adorderid: Int,
//                 adcreativeid: Int,
//                 adplatformproviderid: Int,
//                 sdkversion: String,
//                 adplatformkey: String,
//                 putinmodeltype: Int,
//                 requestmode: Int,
//                 adprice: Double,
//                 adppprice: Double,
//                 requestdate: String,
//                 ip: String,
//                 appid: String,
//                 appname: String,
//                 uuid: String,
//                 device: String,
//                 client: Int,
//                 osversion: String,
//                 density: String,
//                 pw: Int,
//                 ph: Int,
//                 long_s: String,
//                 lat: String,
//                 provincename: String,
//                 cityname: String,
//                 ispid: Int,
//                 ispname: String,
//                 networkmannerid: Int,
//                 networkmannername: String,
//                 iseffective: Int,
//                 isbilling: Int,
//                 adspacetype: Int,
//                 adspacetypename: String,
//                 devicetype: Int,
//                 processnode: Int,
//                 apptype: Int,
//                 district: String,
//                 paymode: Int,
//                 isbid: Int,
//                 bidprice: Double,
//                 winprice: Double,
//                 iswin: Int,
//                 cur: String,
//                 rate: Double,
//                 cnywinprice: Double,
//                 imei: String,
//                 mac: String,
//                 idfa: String,
//                 openudid: String,
//                 androidid: String,
//                 rtbprovince: String,
//                 rtbcity: String,
//                 rtbdistrict: String,
//                 rtbstreet: String,
//                 storeurl: String,
//                 realip: String,
//                 isqualityapp: Int,
//                 bidfloor: Double,
//                 aw: Int,
//                 ah: Int,
//                 imeimd5: String,
//                 macmd5: String,
//                 idfamd5: String,
//                 openudidmd5: String,
//                 androididmd5: String,
//                 imeisha1: String,
//                 macsha1: String,
//                 idfasha1: String,
//                 openudidsha1: String,
//                 androididsha1: String,
//                 uuidunknow: String,
//                 userid: String,
//                 iptype: Int,
//                 initbidprice: Double,
//                 adpayment: Double,
//                 agentrate: Double,
//                 lomarkrate: Double,
//                 adxrate: Double,
//                 title: String,
//                 keywords: String,
//                 tagid: String,
//                 callbackdate: String,
//                 channelid: String,
//                 mediatype: Int
//               )
//
//
//
//
