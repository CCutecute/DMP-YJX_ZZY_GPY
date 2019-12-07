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
  * @date 2019/12/7 14:28
  * @version 1.0
  */
object DeviceTag extends TagTrait{
  override def makeTag(row: Row, dictionary: Map[String, String]): Map[String, Integer] = {
    val client = row.getAs[Int]("client")
    var clientTag = ""
    val networkmannerid = row.getAs[Int]("networkmannerid")
    var networkTag = ""
    val ispid = row.getAs[Int]("ispid")
    var ispTag = ""
    client match {
      case 1 => clientTag = "D00010001"
      case 2 => clientTag = "D00010002"
      case 3 => clientTag = "D00010003"
      case _ => clientTag = "D00010004"
    }
    networkmannerid match {
      case 3 => networkTag = "D00020001"
      case 5 => networkTag = "D00020002"
      case 2 => networkTag = "D00020003"
      case 1 => networkTag = "D00020004"
      case _ => networkTag = "D00020005"
    }
    ispid match {
      case 1 => ispTag = "D00030001"
      case 2 => ispTag = "D00030002"
      case 3 => ispTag = "D00030003"
      case _ => ispTag = "D00030004"
    }
    Map(s"${clientTag}" -> 1,s"${networkTag}" -> 1,s"${ispTag}" -> 1)
  }
}
