package Tags

import constants.ComConstants
import org.apache.spark.sql.Row
import traits.TagMake
import org.apache.commons.lang3.StringUtils
/**
 * @description 应用名称标签
 * @author 郭鹏野
 * @data 2019/12/07.
 * @version 1.0
 */
object AppNameTagMaker extends TagMake{
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    val appName = dictionary.getOrElse(appid, appname)
    if (StringUtils.isNotBlank(appName)) Map(ComConstants.appprefix+ appName -> 1.0) else Map[String, Double]()
  }
}
