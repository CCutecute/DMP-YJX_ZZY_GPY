package dmp.datatreating

import dmp.Entry.Log
import dmp.common.Constant
import dmp.util.SparkUtils
import org.apache.spark.sql.{Dataset}


///**
//  * Description:
//  * Copyright (c),2019,JingxuanYan
//  * This program is protected by copyright laws.
//  *
//  * @author 闫敬轩
//  * @date 2019/12/6 14:02
//  * @version 1.0
//  */

object Treating {
  def main(args: Array[String]): Unit = {

    val ssc = SparkUtils.getSparkSession(false)
    val frame = ssc.read.textFile(Constant.LOG_URL)
    import ssc.implicits._
    val LogDF: Dataset[Log] = frame.map(x => x.split(","))
      .filter(x => x.length == 85)
      .map(line =>
        Log(
          line(0),
          line(1).toInt,
          line(2).toInt,
          line(3).toInt,
          line(4).toInt,
          line(5),
          line(6),
          line(7).toInt,
          line(8).toInt,
          line(9).toDouble,
          line(10).toDouble,
          line(11),
          line(12),
          line(13),
          line(14),
          line(15),
          line(16),
          line(17).toInt,
          line(18),
          line(19),
          if (line(20).isEmpty) -1 else line(20).toInt,
          if (line(21).isEmpty) -1 else line(21).toInt,
          line(22),
          line(23),
          line(24),
          line(25),
          line(26).toInt,
          line(27),
          line(28).toInt,
          line(29),
          line(30).toInt,
          line(31).toInt,
          line(32).toInt,
          line(33),
          line(34).toInt,
          line(35).toInt,
          line(36).toInt,
          line(37),
          line(38).toInt,
          line(39).toInt,
          line(40).toDouble,
          line(41).toDouble,
          line(42).toInt,
          line(43),
          line(44).toDouble,
          line(45).toDouble,
          line(46),
          line(47),
          line(48),
          line(49),
          line(50),
          line(51),
          line(52),
          line(53),
          line(54),
          line(55),
          line(56),
          line(57).toInt,
          line(58).toDouble,
          line(59).toInt,
          line(60).toInt,
          line(61),
          line(62),
          line(63),
          line(64),
          line(65),
          line(66),
          line(67),
          line(68),
          line(69),
          line(70),
          line(71),
          line(72),
          line(73).toInt,
          line(74).toDouble,
          line(75).toDouble,
          line(76).toDouble,
          line(77).toDouble,
          line(78).toDouble,
          line(79),
          line(80),
          line(81),
          line(82),
          line(83),
          line(84).toInt)
      ).distinct()
    LogDF.coalesce(3)
      .write.format("parquet").mode(Constant.SAVE_MODE).save(Constant.LOG_SAVE_URL)

    println("转换完成")
    ssc.stop()
  }
}
