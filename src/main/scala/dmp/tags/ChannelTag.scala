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
  * @date 2019/12/7 14:23
  * @version 1.0
  */
object ChannelTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    Map(s"${Constant.CHANNEL_PREFIX}"+adplatformproviderid -> 1)
  }
}
