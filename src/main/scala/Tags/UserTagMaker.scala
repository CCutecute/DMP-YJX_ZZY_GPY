package Tags

import constants.ComConstants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import traits.TagMake

/**
 * @description ${DESCRIPTION}
 * @author 郭鹏野
 * @data 2019/12/09.
 * @version 1.0
 */
object UserTagMaker extends TagMake {
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {
    ComConstants.idFields.split(",")
      .map(str => {
        var userID = ""
        val info = row.getAs[String](str)
        if (StringUtils.isNotBlank(info)) userID = info else ""
        userID
      })
      .filter(StringUtils.isNotBlank(_))
      .map((_, 0.0))
      .toMap
  }
}
