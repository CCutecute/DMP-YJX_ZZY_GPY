package traits

import org.apache.spark.sql.Row


/**
 * @description ${DESCRIPTION}
 * @author 郭鹏野
 * @data 2019/12/07.
 * @version 1.0
 */
trait TagMake {
  def TagMaker(row: Row,dictionary:Map[String,String] = null):Map[String,Double]
}
