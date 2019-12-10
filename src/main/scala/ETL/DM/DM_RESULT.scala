package ETL.DM

import MyUdf.myudf
import constants.ComConstants
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, sum, when}

/**
 * @description DM数据集市层指标数据计算过程
 * @author 郭鹏野
 * @data 2019/12/07.
 * @version 1.0
 */
object DM_RESULT {
  def dm_area(frame: DataFrame, agg: String*): DataFrame = {

    frame
      .groupBy(agg(0), agg(1))
      .agg(
        count(when(col(ComConstants.REQUEST_MODE) === 1,1)).alias("原始请求数"),
        count(when(
          col(ComConstants.PROCESS_NODE) >= 2
            && col(ComConstants.REQUEST_MODE) === 1,
          1)).alias("有效请求数"),
        count(when(
          col(ComConstants.PROCESS_NODE) === 3
            && col(ComConstants.REQUEST_MODE) === 1,
          1)).alias("广告请求数"),
        count(when(
          col(ComConstants.IS_EFFECTIVE) === 1
            && col(ComConstants.IS_BILLING) === 1
            && col(ComConstants.IS_BID) === 1,
          1)).alias("参与竞价数"),
        count(when(
          col(ComConstants.IS_EFFECTIVE) === 1
            && col(ComConstants.IS_BILLING) === 1
            && col(ComConstants.IS_WIN) === 1
            && col(ComConstants.AD_ORDER_ID) =!= 1,
          1)).alias("竞价成功数"),
        count(when(
          col(ComConstants.IS_EFFECTIVE) === 1
            && col(ComConstants.IS_BILLING) === 1
            && col(ComConstants.IS_WIN) === 1
            && col(ComConstants.AD_ORDER_ID) =!= 1
            && col(ComConstants.REQUEST_MODE) === 2,
          1)).alias("展示量"),
        count(when(
          col(ComConstants.IS_EFFECTIVE) === 1
            && col(ComConstants.REQUEST_MODE) === 3,
          1)).alias("点击量"),
        sum(when(
          col(ComConstants.IS_EFFECTIVE) === 1
            && col(ComConstants.IS_BILLING) === 1
            && col(ComConstants.IS_WIN) === 1,
          col("winprice"))).alias("广告消费"),
        sum(when(
          col(ComConstants.IS_EFFECTIVE) === 1
            && col(ComConstants.IS_BILLING) === 1
            && col(ComConstants.IS_WIN) === 1,
          col("adpayment"))).alias("广告成本")
      )
      .withColumn("竞价成功率", myudf.simRate(col("参与竞价数"), col("竞价成功数")))
      .withColumn("点击率", myudf.simRate(col("展示量"), col("点击量")))
      .select(DM_Helper.select_dm_expr().head,DM_Helper.select_dm_expr().tail:_*)
  }
}
