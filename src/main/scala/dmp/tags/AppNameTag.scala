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
  * @date 2019/12/7 11:50
  * @version 1.0
  */
object AppNameTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    val appid = row.getAs[String]("appid")
    val appName = dictionary.getOrElse(appid,"")
    if(appName nonEmpty) Map[String,Integer](s"${Constant.APP_PREFIX}" + appName -> 1)
    else Map()
  }
}
