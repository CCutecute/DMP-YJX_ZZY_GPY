package Utils

import constants.ComConstants
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description 数据初始化，使用kyro序列化方式，转换成parquet文件
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
case class DataInit(
                     sessionid: String,
                     advertisersid: Int,
                     adorderid: Int,
                     adcreativeid: Int,
                     adplatformproviderid: Int,
                     sdkversion: String,
                     adplatformkey: String,
                     putinmodeltype: Int,
                     requestmode: Int,
                     adprice: Double,
                     adppprice: Double,
                     requestdate: String,
                     ip: String,
                     appid: String,
                     appname: String,
                     uuid: String,
                     device: String,
                     client: Int,
                     osversion: String,
                     density: String,
                     pw: Int,
                     ph: Int,
                     lon: String,
                     lat: String,
                     provincename: String,
                     cityname: String,
                     ispid: Int,
                     ispname: String,
                     networkmannerid: Int,
                     networkmannername: String,
                     iseffective: Int,
                     isbilling: Int,
                     adspacetype: Int,
                     adspacetypename: String,
                     devicetype: Int,
                     processnode: Int,
                     apptype: Int,
                     district: String,
                     paymode: Int,
                     isbid: Int,
                     bidprice: Double,
                     winprice: Double,
                     iswin: Int,
                     cur: String,
                     rate: Double,
                     cnywinprice: Double,
                     imei: String,
                     mac: String,
                     idfa: String,
                     openudid: String,
                     androidid: String,
                     rtbprovince: String,
                     rtbcity: String,
                     rtbdistrict: String,
                     rtbstreet: String,
                     storeurl: String,
                     realip: String,
                     isqualityapp: Int,
                     bidfloor: Double,
                     aw: Int,
                     ah: Int,
                     imeimd5: String,
                     macmd5: String,
                     idfamd5: String,
                     openudidmd5: String,
                     androididmd5: String,
                     imeisha1: String,
                     macsha1: String,
                     idfasha1: String,
                     openudidsha1: String,
                     androididsha1: String,
                     uuidunknow: String,
                     userid: String,
                     iptype: Int,
                     initbidprice: Double,
                     adpayment: Double,
                     agentrate: Double,
                     lomarkrate: Double,
                     adxrate: Double,
                     title: String,
                     keywords: String,
                     tagid: String,
                     callbackdate: String,
                     channelid: String,
                     mediatype: Int
                   )

object CSV2ParquetFile {
  def CSV2Parquet(spark: SparkSession, PathCSV: String, PathParquet: String): Unit = {
    import spark.implicits._
    val df: DataFrame = spark.sparkContext.textFile(PathCSV).map(
      _.split(",", -1)
    ).map(x => {

      //if(x.length==85)
      for (i <- 0 to 84) {
        if (i == 0 || i >= 5 && i <= 6 || i >= 11 && i <= 16 || i == 18 || i == 19 || i >= 22 && i <= 25 || i == 27 || i == 29
          || i == 33 || i == 37 || i == 43 || i >= 46 && i <= 56 || i >= 61 && i <= 72 || i >= 79 && i <= 83) {
          if (x(i) == "") {
            x(i) = ""
          }
        } else {
          if (x(i) == "")
            x(i) = "0"
        }
      }
      DataInit(x(0), x(1).trim.toInt, x(2).trim.toInt, x(3).trim.toInt, x(4).trim.toInt, x(5), x(6), x(7).trim.toInt, x(8).trim.toInt, x(9).trim.toDouble,
        x(10).trim.toDouble, x(11), x(12), x(13), x(14), x(15), x(16), x(17).trim.toInt, x(18), x(19), x(20).trim.toInt, x(21).trim.toInt, x(22), x(23),
        x(24), x(25), x(26).trim.toInt, x(27), x(28).trim.toInt, x(29), x(30).trim.toInt, x(31).trim.toInt, x(32).trim.toInt, x(33), x(34).trim.toInt,
        x(35).trim.toInt, x(36).trim.toInt, x(37), x(38).trim.toInt, x(39).trim.toInt, x(40).trim.toDouble, x(41).trim.toDouble, x(42).trim.toInt, x(43),
        x(44).trim.toDouble, x(45).trim.toDouble, x(46), x(47), x(48), x(49), x(50), x(51), x(52), x(53), x(54), x(55), x(56), x(57).trim.toInt, x(58).trim.toDouble,
        x(59).trim.toInt, x(60).trim.toInt, x(61), x(62), x(63), x(64), x(65), x(66), x(67), x(68), x(69), x(70), x(71), x(72), x(73).trim.toInt, x(74).trim.toDouble,
        x(75).trim.toDouble, x(76).trim.toDouble, x(77).trim.toDouble, x(78).trim.toDouble, x(79), x(80), x(81), x(82), x(83), x(84).trim.toInt)
    }).toDF
    df.write.parquet(PathParquet)
  }
}