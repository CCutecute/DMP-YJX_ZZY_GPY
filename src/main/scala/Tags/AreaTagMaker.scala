package Tags
import constants.ComConstants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * @description ${DESCRIPTION}
 * @author 郭鹏野
 * @data 2019/12/10.
 * @version 1.0
 */
object AreaTagMaker extends traits.TagMake {
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {
    var map = Map[String, Double]()
    val province: String = row.getAs[String]("provincename")
    if (StringUtils.isNotBlank(province)){
      map += (ComConstants.Provinceprefix + province -> 1.0)
    }

    val city: String = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(city)){
      map += (ComConstants.Cityprefix + city -> 1.0)
    }

    map
  }
}
