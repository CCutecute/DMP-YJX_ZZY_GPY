package dmp.tags

import dmp.common.Constant
import dmp.traits.TagTrait
import org.apache.spark.sql.Row

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/7 15:49
  * @version 1.0
  */
object RegionTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    Map(s"${Constant.REGION_PROVINCE_PREFIX}"+provincename -> 1,s"${Constant.REGION_CITY_PREFIX}"+cityname -> 1)
  }
}
