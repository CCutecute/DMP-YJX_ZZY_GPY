package dmp.traits

import org.apache.spark.sql.Row

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/7 10:29
  * @version 1.0
  */
trait TagTrait {
  def makeTag(row: Row,dictionary:Map[String,String] = null):Map[String,Integer]
}
