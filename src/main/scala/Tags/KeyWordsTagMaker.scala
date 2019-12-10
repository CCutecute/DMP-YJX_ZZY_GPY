package Tags

import constants.ComConstants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * @description 关键字标签
 * @author 郭鹏野
 * @data 2019/12/10.
 * @version 1.0
 */
object KeyWordsTagMaker extends traits.TagMake {
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {
    var map = Map[String, Double]()
    //拿到关键字字段
    val keywords: String = row.getAs[String]("keywords")
    //把每一个关键字用|分开，找到小于8且大于3个字符的关键字，拼接成关键字标签
    if (StringUtils.isNotBlank(keywords)) {
      val fileds = keywords.split("[|]")
      map = fileds.filter(words => {
        words.length >= 3 && words.length <= 8
      }).map(words => {
        (ComConstants.keywordprefix + words.replace(":", ""), 1.0)
      }).toMap[String, Double]
    }
    map
  }
}
