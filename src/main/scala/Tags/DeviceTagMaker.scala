package Tags
import constants.ComConstants
import org.apache.spark.sql.Row

/**
 * @description 给设备打标签：
 *             1.设备操作系统
 *             2.设备联网方式
 *             3.设备运营商
 * @author 郭鹏野
 * @data 2019/12/10.
 * @version 1.0
 */
object DeviceTagMaker extends traits.TagMake {

  /**
   *
   * @param row
   * @param dictionary
   * @return
   */
  override def TagMaker(row: Row, dictionary: Map[String, String]): Map[String, Double] = {


    var map = Map[String,Double]()
    val client = row.getAs[Int]("client")
    val networkmannername = row.getAs[String]("networkmannername")
    val ispname = row.getAs[String]("ispname")

    val os = dictionary.getOrElse(client.toString,dictionary("4"))
    map += (ComConstants.osprefix + os -> 1.0)
    val network = dictionary.getOrElse(networkmannername.toString,dictionary("NETWORKOTHER"))
    map += (ComConstants.networkprefix + network -> 1.0)
    val isp = dictionary.getOrElse(ispname.toString,dictionary("OPERATOROTHER"))
    map += (ComConstants.ispprefix + isp -> 1.0)


    map
  }

}
