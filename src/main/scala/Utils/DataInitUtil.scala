package Utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * @description
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object DataInitUtil {

  def CSV2Parquet(spark:SparkSession,PathCSV:String,PathParquet:String): Unit ={

    val schema = StructType(
      StructField("sessionid", StringType)::
        StructField("advertisersid", IntegerType)::
        StructField("adorderid", IntegerType)::
        StructField("adcreativeid", IntegerType)::
        StructField("adplatformproviderid", IntegerType)::
        StructField("sdkversion", StringType)::
        StructField("adplatformkey", StringType)::
        StructField("putinmodeltype", IntegerType)::
        StructField("requestmode", IntegerType)::
        StructField("adprice", DoubleType)::
        StructField("adppprice", DoubleType)::
        StructField("requestdate", StringType)::
        StructField("ip", StringType)::
        StructField("appid", StringType)::
        StructField("appname", StringType)::
        StructField("uuid", StringType)::
        StructField("device", StringType)::
        StructField("client", IntegerType)::
        StructField("osversion", StringType)::
        StructField("density", StringType)::
        StructField("pw", IntegerType)::
        StructField("ph", IntegerType)::
        StructField("long", StringType)::
        StructField("lat", StringType)::
        StructField("provincename", StringType)::
        StructField("cityname", StringType)::
        StructField("ispid", IntegerType)::
        StructField("ispname", StringType)::
        StructField("networkmannerid", IntegerType)::
        StructField("networkmannername",StringType)::
        StructField("iseffective", IntegerType)::
        StructField("isbilling", IntegerType)::
        StructField("adspacetype", IntegerType)::
        StructField("adspacetypename", StringType)::
        StructField("devicetype", IntegerType)::
        StructField("processnode", IntegerType)::
        StructField("apptype", IntegerType)::
        StructField("district", StringType)::
        StructField("paymode", IntegerType)::
        StructField("isbid", IntegerType)::
        StructField("bidprice", DoubleType)::
        StructField("winprice", DoubleType)::
        StructField("iswin", IntegerType)::
        StructField("cur", StringType)::
        StructField("rate", DoubleType)::
        StructField("cnywinprice", DoubleType)::
        StructField("imei", StringType)::
        StructField("mac", StringType)::
        StructField("idfa", StringType)::
        StructField("openudid", StringType)::
        StructField("androidid", StringType)::
        StructField("rtbprovince", StringType)::
        StructField("rtbcity", StringType)::
        StructField("rtbdistrict", StringType)::
        StructField("rtbstreet", StringType)::
        StructField("storeurl", StringType)::
        StructField("realip", StringType)::
        StructField("isqualityapp", IntegerType)::
        StructField("bidfloor", DoubleType)::
        StructField("aw", IntegerType)::
        StructField("ah", IntegerType)::
        StructField("imeimd5", StringType)::
        StructField("macmd5", StringType)::
        StructField("idfamd5", StringType)::
        StructField("openudidmd5", StringType)::
        StructField("androididmd5", StringType)::
        StructField("imeisha1", StringType)::
        StructField("macsha1", StringType)::
        StructField("idfasha1", StringType)::
        StructField("openudidsha1", StringType)::
        StructField("androididsha1", StringType)::
        StructField("uuidunknow", StringType)::
        StructField("userid", StringType)::
        StructField("iptype", IntegerType)::
        StructField("initbidprice", DoubleType)::
        StructField("adpayment", DoubleType)::
        StructField("agentrate", DoubleType)::
        StructField("lomarkrate", DoubleType)::
        StructField("adxrate", DoubleType)::
        StructField("title", StringType)::
        StructField("keywords", StringType)::
        StructField("tagid", StringType)::
        StructField("callbackdate", StringType)::
        StructField("channelid", StringType)::
        StructField("mediatype", IntegerType)::
        Nil
    )
    val df = spark.read.schema(schema).option("header", true).csv(PathCSV)

    df.write.parquet(PathParquet)
  }
}
