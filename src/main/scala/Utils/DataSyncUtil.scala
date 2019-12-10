package Utils

import java.util.Properties

import constants.ComConstants
import org.apache.spark.sql.DataFrame

/**
 * @description ${DESCRIPTION}
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object DataSyncUtil {
  def result2mysql(frame:DataFrame,TableName:String): Unit ={
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user","root")
    connectionProperties.setProperty("password","123456")
    connectionProperties.setProperty("url","jdbc:mysql://localhost:3306/sparks?useUnicode=true&characterEncoding=UTF8&autoReconnect=true&zeroDateTimeBehavior=convertToNull")
    val url = "jdbc:mysql://localhost:3306/sparks?useUnicode=true&characterEncoding=UTF8&autoReconnect=true&zeroDateTimeBehavior=convertToNull"
    frame
      .coalesce(1)
      .write
      .mode(ComConstants.DEF_SAVE_MODE)
      //.partitionBy()
      .jdbc(url,TableName,connectionProperties)

  }

  def outputasjson(frame: DataFrame,Path:String): Unit ={
    frame.coalesce(1).write.json(Path)
  }
}
