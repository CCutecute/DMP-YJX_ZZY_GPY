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
  * @date 2019/12/7 14:46
  * @version 1.0
  */
object KeyWordTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    var map = Map[String,Integer]()
    val keywords = row.getAs[String]("keywords")
    val splited: Array[String] = keywords.split("\\|")
    for(keyword <- splited){
      if(!dictionary.contains(keyword)){
        if(keyword.nonEmpty){
          map += (s"${Constant.KEY_WORD_PREFIX}"+keyword -> 1)}
      }
    }
    map
  }
}
