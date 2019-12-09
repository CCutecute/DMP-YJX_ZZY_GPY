package dmp.util

import com.alibaba.fastjson.{JSON, JSONObject}
import dmp.common.Constant

/**
  * 请求高德地图获取商圈
  */
object AmapUtil {

  def getBusinesss(long:Double,lat:Double):String={
    // 加载配置项
    //&key=xxx&location=xx,xx"
    val location = long +","+lat
    val url = Constant.GAODE_API_URL+"&key="+Constant.GAODE_KEY+"&location="+location
    // 调用地图 发送http请求
    val json = HttpUtil.get(url)
    // 解析json
    val jsonObject = JSON.parseObject(json)
    // 创建返回值集合
    val buffer = collection.mutable.ListBuffer[String]()
    // 获取状态码
    val status = jsonObject.getIntValue("status")
    // 判断状态码 1 正确 0 错误
    if(status == 0) return  null
    // 如果不为空 进行取值操作
    val regeocodeJson = jsonObject.getJSONObject("regeocode")
    if(regeocodeJson == null) return null
    val addressComponent = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponent == null) return null
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null) return null
    // 循环处理json内的数组
    for (i<-businessAreas.toArray){
      if(i.isInstanceOf[JSONObject]){
        val json = i.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
