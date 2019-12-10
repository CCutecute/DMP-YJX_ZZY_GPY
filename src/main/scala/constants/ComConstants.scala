package constants

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
 * @description 常量池
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object ComConstants {
  val AppName = "DMP_AD_Push"
  val PathCSV = "E:\\大数据学习资料\\项目相关\\DMP项目资料\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH"
  val PathParquet = "E:\\大数据学习资料\\项目相关\\DMP项目资料\\parquetFile"
  val PathJson = "E:\\大数据学习资料\\项目相关\\DMP项目资料\\jsonFile"
  val DEF_SAVE_MODE = SaveMode.Overwrite
  val DEF_CACHE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK_2

  //维度字段
  val PROVINCE = "provincename"
  val CITY = "cityname"


  //度量字段
  val REQUEST_MODE = "requestmode"
  val PROCESS_NODE = "processnode"
  val IS_EFFECTIVE = "iseffective"
  val IS_BILLING = "isbilling"
  val IS_BID = "isbid"
  val IS_WIN = "iswin"
  val AD_ORDER_ID = "adorderid"

  //指标
  val RAW_REQUEST = "原始请求数"
  val VALID_REQUEST = "有效请求数"
  val AD_REQUEST = "广告请求数"
  val BIDDING_NUM = "参与竞价数"
  val SUCESS_NUM = "竞价成功数"
  val SUCCESS_RATE = "竞价成功率"
  val SHOW_NUM = "展示量"
  val CLICK_NUM = "点击量"
  val CLICK_RATE = "点击率"
  val AD_COST = "广告成本"
  val AD_SPEND = "广告消费"

  //前后缀
  val adprefix = "Adtype:@"
  val appprefix = "AppName:@"
  val channelprefix = "Channel:@"
  val osprefix = "OS:@"
  val networkprefix = "NetWork:@"
  val ispprefix = "ISP:@"
  val keywordprefix = "KeyWords:@"
  val Provinceprefix = "Province:@"
  val Cityprefix = "City:@"

  //过滤
  lazy val idFields: String = "imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1"
  lazy val filterSQL: String = idFields
    .split(",")
    .map(field => s"$field is not null")
    .mkString(" or ")
}
