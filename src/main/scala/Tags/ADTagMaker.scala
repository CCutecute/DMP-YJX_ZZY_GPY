package Tags

import constants.ComConstants
import org.apache.spark.sql.Row
import traits.TagMake

import scala.collection.JavaConverters._

/**
 * @description 广告类型标签（横幅型，插屏型，全屏型）
 * @author 郭鹏野
 * @data 2019/12/07.
 * @version 1.0
 */
object ADTagMaker extends TagMake {
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {
    val ADType = row.getAs[Int]("adspacetype")
    ADType match {
      case 1 => Map(ComConstants.adprefix + "banner" -> 1.0)
      case 2 => Map(ComConstants.adprefix + "插屏" -> 1.0)
      case 3 => Map(ComConstants.adprefix + "全屏" -> 1.0)
      case _ => Map[String,Double]()
    }
  }
}
