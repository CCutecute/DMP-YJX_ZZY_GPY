package Tags
import constants.ComConstants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * @description 渠道标签
 * @author 郭鹏野
 * @data 2019/12/10.
 * @version 1.0
 */
object SourcesTagMaker extends traits.TagMake {
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {
    val channelid: String = row.getAs[String]("channelid")
    if(StringUtils.isNotBlank(channelid)){
      Map[String,Double](ComConstants.channelprefix+channelid -> 1.0)
    }else{
      Map[String,Double]()
    }
  }
}
