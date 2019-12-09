package dmp.common

import org.apache.log4j.Logger
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

  val LOG_URL = "D://XSQ-BigData-24/项目/DMP项目资料/2016-10-01_06_p1_invalid.1475274123982.log.FINISH"
  val LOG_SAVE_URL = "D://XSQ-BigData-24/项目/DMP项目资料/2016-10-01_06_p1_invalid.1475274123982.log.FINISH.parquet"
  val VOLUMN_SAVE_URL = "D://XSQ-BigData-24/项目/DMP项目资料/3.2.0Volumn.log"
  val APP_DICT_URL = "D:\\XSQ-BigData-24\\项目\\DMP项目资料/app_dict.txt"
  val STOPWORDS_URL = "D:\\XSQ-BigData-24\\项目\\DMP项目资料/stopwords.txt"

  val DB_URL ="jdbc:mysql://127.0.0.1:3306/advertising?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC"
  val DB_USERNAME="root"
  val DB_PASSWORD="123321"
  val DB_VOLUMN = "volumn"

  val SERIALIZER ="org.apache.spark.serializer.KryoSerializer"
  val SAVE_MODE = SaveMode.Overwrite

  val AD_PREFIX = "LN"
  val APP_PREFIX = "APP"
  val CHANNEL_PREFIX = "CN"
  val KEY_WORD_PREFIX = "K"
  val REGION_PROVINCE_PREFIX = "ZP"
  val REGION_CITY_PREFIX = "ZC"

  val HBASE_CONNECT_KEY = "hbase.zookeeper.quorum"
  val HBASE_CONNECT_VALUE = "hadoop04:2181"

  val JEDIS_MAX_IDLE = 100
  val JEDIS_MAX_TOTAL = 300
  val JEDIS_MIN_IDLE = 1
  val JEDIS_TIME_OUT = 30000
  val JEDIS_HOST = "hadoop04"
  val JEDIS_PORT = 6379
  val JEDIS_ON_BORROW = true
  val JEDIS_TRADING_AREA_TABLE = "tradingArea"

  val GAODE_API_URL = "https://restapi.amap.com/v3/geocode/regeo?output=json&radius=5000&extensions=all"
  val GAODE_KEY = "aa9aefe13d7fa17a7d84f26fe1bf0003"
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
