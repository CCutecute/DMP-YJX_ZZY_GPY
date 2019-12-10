package Tags

import constants.ComConstants
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}
import traits.Process

import scala.io.Source._


/**
 * @description ${DESCRIPTION}
 * @author 郭鹏野
 * @data 2019/12/07.
 * @version 1.0
 */
object TagMakerDemo extends Process {
  override def Processor(spark: SparkSession, frame: DataFrame): Unit = {
    //将APP文件整合成map
    import spark.implicits._
    try {
      val dicfile = fromFile("data/dicapp")
      val appNameMap = dicfile
        .getLines
        .toList
        .map(line => {
          val strings: Array[String] = line.split("##")
          (strings(0), strings(1))
        }).toMap

      val devceFile = fromFile(name = "data/dicdevice")
      val deviceMap = devceFile
        .getLines
        .toList
        .map(str => {
          (str.split("##")(0), str.split("##")(1))
        })
        .toMap
      //将appNameMap广播
      val appNameBroadcast = spark.sparkContext.broadcast(appNameMap)

      //将 device 文件 整为 广播变量
      val deviceBroadcast = spark.sparkContext.broadcast(deviceMap)
      //过滤掉十五个ID字段都为空的信息
      //给每行打标签

      frame
        .where(ComConstants.filterSQL)
        .rdd
        .map(row => {
          //用户标签
          val userTag = UserTagMaker.TagMaker(row)
          //行为标签
          val adTypeTag = ADTagMaker.TagMaker(row)
          val AppNameTag = AppNameTagMaker.TagMaker(row, appNameBroadcast.value)
          val ChannelTag = SourcesTagMaker.TagMaker(row)
          val Devicetag = DeviceTagMaker.TagMaker(row, deviceBroadcast.value)
          val KeyWordsTag = KeyWordsTagMaker.TagMaker(row)
          val AreaTag = AreaTagMaker.TagMaker(row)
          val tags = adTypeTag ++ AppNameTag ++ ChannelTag ++ Devicetag ++ KeyWordsTag ++ AreaTag
          (userTag.keys.toList, tags.toList)
        })
        //过滤掉key为空的
        .filter(x => x._1.nonEmpty)
        //将权值相加得到权重（先reduceByKey将行为标签聚合，再从value中得到权值的和）
        .reduceByKey(
          (x, y) => (x ++ y)
            .groupBy(_._1)
            .mapValues(_.map(_._2).sum)
            .toList
            .sortBy(_._1)
        ) //.foreach(println)
//        .toDF("UserID", "Tags")
      //        .show(1000)
//        .map{
//          case (userid,userTags)=>{
//            val put = new Put(Bytes.toBytes(userid))
//            val tags = userTags.map(t=>t._1+":"+t._2).mkString(",")
//            // 对应的rowKey下面的列  列名 列的value值
//            put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(tags))
//            // 存入Hbase
//            (new ImmutableBytesWritable(),put)
//          }
//        }
      //将数据写入Hbase
    } finally {
      spark.close()
    }
  }
}
