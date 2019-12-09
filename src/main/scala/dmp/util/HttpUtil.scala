package dmp.util

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
object HttpUtil {
  /**
    * GET请求
    * @param url
    * @param header
    * @return json字符串
    */
  def get(url: String, header: String = null): String  ={
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet()
        .toArray.map(_.toString)
        .foreach(key => httpGet.setHeader(key, json.getString(key)))
    }
    // 发送请求
    val response: CloseableHttpResponse = httpClient.execute(httpGet)
    // 获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")
  }


  /**
    * POST请求
    * @param url
    * @param params
    * @param header
    * @return json字符串
    */
  def post(url: String, params: String = null, header: String = null): String ={
    val httpClient = HttpClients.createDefault()    // 创建 client 实例
    val post = new HttpPost(url)    // 创建 post 实例

    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet()
        .toArray
        .map(_.toString)
        .foreach(key => post.setHeader(key, json.getString(key)))
    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }
    // 创建 client 实例
    val response: CloseableHttpResponse = httpClient.execute(post)
    // 获取返回结果
    EntityUtils.toString(response.getEntity, "UTF-8")
  }
}
