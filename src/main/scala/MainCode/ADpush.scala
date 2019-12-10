package MainCode

import Utils.{CSV2ParquetFile, DataInitUtil, DataSyncUtil}
import constants.ComConstants
import org.apache.spark.sql.DataFrame
import ETL.DM.DM_RESULT

/**
 * @description DMP广告投放项目
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object ADpush {

  def main(args: Array[String]): Unit = {

    val spark = SessionHander.SessionWithLocalAndKyro(ComConstants.AppName)
    //DataInitUtil.CSV2Parquet(spark,ComConstants.PathCSV,ComConstants.PathParquet) //默认java序列化
    //CSV2ParquetFile.CSV2Parquet(spark,ComConstants.PathCSV,ComConstants.PathParquet) //使用kyro序列化
    val frame: DataFrame = spark.read.parquet(ComConstants.PathParquet).persist(ComConstants.DEF_CACHE_LEVEL)
    //val resFrame: DataFrame = DM_RESULT.dm_area(frame,ComConstants.PROVINCE,ComConstants.CITY)
    //DataSyncUtil.outputasjson(resFrame,ComConstants.PathJson)
    //resFrame.show(100)
    Tags.TagMakerDemo.Processor(spark,frame)
  }
}
