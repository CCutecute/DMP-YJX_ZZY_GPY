package dmp.tags


import ch.hsr.geohash.GeoHash
import dmp.traits.TagTrait
import dmp.util.{AmapUtil, RedisUtils}
import org.apache.spark.sql.Row

/**
  * Description:
  * Copyright (c),2019,JingxuanYan
  * This program is protected by copyright laws.
  *
  * @author 闫敬轩
  * @date 2019/12/9 14:56
  * @version 1.0
  */
object TradingAreaTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    val long_s = row.getAs[String]("long_s")
    val lat = row.getAs[String]("lat")
    //73°33′E至135°05′E。纬度范围：3°51′N至53°33′N
    if(long_s.nonEmpty && lat.nonEmpty){
      val longdle= long_s.toDouble
      val latdle = lat.toDouble
      if(longdle >= 73.66 && longdle <= 135.05 && latdle >= 3.86 && latdle <= 53.55) {
        val precision = 6
        val geoHash: GeoHash = GeoHash.withCharacterPrecision(latdle, longdle, precision)
        val hashCode: String = geoHash.toBase32 // 使用给定的经纬度坐标生成的Geohash字符编码
//        println(hashCode)
        val busiName = RedisUtils.readRedis(hashCode)
        if(busiName != null){
          println(busiName)
          Map(busiName -> 1)
        }else{
          //根据geohash向高德请求businame，存入redis
          val businames: String = AmapUtil.getBusinesss(longdle,latdle)
          var map = Map[String,Integer]()
         for(elem <- businames.split(",")){
           RedisUtils.writeRedis(hashCode,elem)
           map += (elem -> 1)
         }
//          println(businames)
          map
        }
      }
    }
    Map()
  }
}
