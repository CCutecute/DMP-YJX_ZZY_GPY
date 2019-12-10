package MyUdf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
/**
 * @description 自定义函数库
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object myudf {
  val simpleRate: (Int, Int) => String = (bidding:Int, bidsucees:Int) =>{
    val rate: Double = bidsucees.toDouble/bidding.toDouble*100.0
    //val rate: Double = 56.toDouble/123.toDouble*100
    rate.formatted("%.2f").toString.concat("%")
  }
  val simRate: UserDefinedFunction = udf(simpleRate)
}
