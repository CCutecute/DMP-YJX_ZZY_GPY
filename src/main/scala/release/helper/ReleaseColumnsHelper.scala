package release.helper

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/11/28 15:33
  * @version 1.0
  */
object ReleaseColumnsHelper {




  def selectODSReleaseCustomerColumns(spark:SparkSession):ArrayBuffer[Column] = {

    import spark.implicits._
    val columns = new ArrayBuffer[Column]()
    columns += $"release_session"
    columns += $"release_status"
    columns += $"device_num"
    columns += $"device_type"
    columns += $"sources"
    columns += $"channels"
    columns += get_json_object(col("exts"),"$.idcard").as("idcard")
    columns += (substring(year(current_date())-regexp_extract(get_json_object(col("exts"),"$.idcard"),"""(\d{6})(\d{4})(\d{4})""",2),0,2)).as("age")
    columns += (substring(regexp_extract(get_json_object(col("exts"),"$.idcard"),"""(\d{6})(\d{8})(\d{4})""",3)%2,0,1)).as("gender")
    columns += get_json_object(col("exts"),"$.area_code").as("area_code")
    columns += get_json_object(col("exts"),"$.longitude").as("longitude")
    columns += get_json_object(col("exts"),"$.latitude").as("latitude")
    columns += get_json_object(col("exts"),"$.matter_id").as("matter_id")
    columns += get_json_object(col("exts"),"$.model_code").as("model_code")
    columns += get_json_object(col("exts"),"$.model_version").as("model_version")
    columns += get_json_object(col("exts"),"$.aid").as("aid")
    columns += $"ct"
    columns
  }

  def selectDWReleaseCustomerColumns(spark:SparkSession):ArrayBuffer[Column] = {
    import spark.implicits._
    val columns = new ArrayBuffer[Column]()
    columns += $"release_session"
    columns += $"release_status"
    columns += $"device_num"
    columns += $"device_type"
    columns += $"sources"
    columns += $"channels"
    columns += $"idcard"
    columns += $"age"
    columns += $"gender"
    columns += $"area_code"
    columns += $"longitude"
    columns += $"latitude"
    columns += $"matter_id"
    columns += $"model_code"
    columns += $"model_version"
    columns += $"aid"
    columns += $"ct"
    columns += $"dt"
    columns
  }

  def selectDMCustomerSourcesColumns(spark:SparkSession):ArrayBuffer[Column] ={
    import spark.implicits._
    val columns = new ArrayBuffer[Column]()
    columns += $"dt"
    columns += $"sources"
    columns += $"user_count"
    columns += $"total_count"
    columns
  }

}
