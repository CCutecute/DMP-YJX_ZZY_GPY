package dmp.report

import java.util.Properties

import dmp.common.Constant
import dmp.util.SparkUtils
import org.apache.calcite.avatica.Meta.ConnectionProperties
import org.apache.spark.sql.DataFrame

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/6 18:34
  * @version 1.0
  */
object Volumn {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession(false)
    val df: DataFrame = spark.read.parquet(Constant.LOG_SAVE_URL).persist()
//    df.show()
    import org.apache.spark.sql.functions._
    val volumnDF: DataFrame = df
      .groupBy("provincename", "cityname")
      .agg(count("sessionid").as("ct"))
      .select("ct", "provincename", "cityname")
//        .show(200)

    volumnDF.coalesce(1).write.format("json").mode(Constant.SAVE_MODE).save(Constant.VOLUMN_SAVE_URL)
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", Constant.DB_USERNAME);// 设置用户名
    connectionProperties.setProperty("password", Constant.DB_PASSWORD);// 设置密码
    volumnDF.write.jdbc(Constant.DB_URL,Constant.DB_VOLUMN,connectionProperties)
    println("json写入完成")
    spark.stop()
  }
}
