package dmp.report

import dmp.common.Constant
import dmp.util.SparkUtils
import org.apache.spark.sql.DataFrame

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/6 19:16
  * @version 1.0
  */
object Yunying {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession(false)
    val df: DataFrame = spark.read.parquet(Constant.LOG_SAVE_URL).persist()
    df.createOrReplaceTempView("tb_log")
    val regionDF = spark.sql(
      """
        |select
        |provincename,cityname,
        |OriginalRequest,ValidRequest,adRequest,bidsNum,bidsSus,
        |bidsSus / bidsNum,adDisplayNum,adClickNum,
        |adDisplayNum / adClickNum,adconsume,adcost
        |from
        |(select
        |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as OriginalRequest,
        |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as ValidRequest,
        |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) as adRequest,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as bidsNum,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) as bidsSus,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as adDisplayNum,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as adClickNum,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) as adconsume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) as adcost
        |from tb_log
        |group by provincename, cityname) t
      """.stripMargin)
    regionDF.show()

    spark.stop()
  }
}
