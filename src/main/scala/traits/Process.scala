package traits

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description ${DESCRIPTION}
 * @author 郭鹏野
 * @data 2019/12/07.
 * @version 1.0
 */
trait Process {
  def Processor(spark:SparkSession,frame:DataFrame){}
}
