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
  * @date 2019/12/7 10:42
  * @version 1.0
  */

/**
  * 1)	广告位类型（标签格式： LC03->1 或者 LC16->1）xx 为数字，小于 10 补 0，把广告位类型名称，LN 插屏->1
  */
object ADTypeTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    val adspacetype = row.getAs[Int]("adspacetype")
    adspacetype match {
      case 1 => Map(s"${Constant.AD_PREFIX}banner" -> 1)
      case 2 => Map(s"${Constant.AD_PREFIX}插屏" -> 1)
      case 3 => Map(s"${Constant.AD_PREFIX}全屏" -> 1)
      case _ => Map[String, Integer]()
    }
  }
}
