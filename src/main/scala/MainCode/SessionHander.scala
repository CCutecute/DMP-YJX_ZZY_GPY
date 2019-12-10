package MainCode

import Utils.DataInit
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @description create a Spark Session with Kryo\local\hive
 * @author 郭鹏野
 * @data 2019/12/06.
 * @version 1.0
 */
object SessionHander {


  // Make sure to set these properties *before* creating a SparkContext!
  def SessionWithLocalAndKyro(AppName:String): SparkSession ={
    val spark = SparkSession
      .builder()
      //dynamicAllocation.enabled
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // use this if you need to increment Kryo buffer size. Default 64k
      .config("spark.kryoserializer.buffer", "1024k")
      // use this if you need to increment Kryo buffer max size. Default 64m
      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.kryo.registrationRequired", "true")
      .config(getConfig)
      .appName(AppName)
      .master("local[*]")
      .getOrCreate()
    spark
  }

//本地sparksession
  def SessionWithLocal(AppName:String): SparkSession ={
    val spark = SparkSession
      .builder()
      //dynamicAllocation.enabled
      .config("spark.dynamicAllocation.enabled","true")
      .config(getConfig)
      .appName(AppName)
      .master("local[*]")
      .getOrCreate()
    spark
  }

  //hiveSession
  def SessionWithHive(AppName:String): SparkSession ={
    val spark = SparkSession
      .builder()
      //dynamicAllocation.enabled
      .config("spark.dynamicAllocation.enabled","true")
      .config(getConfig)
      .appName(AppName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def SessionWithHiveAndKyro(AppName:String): SparkSession ={
    val spark = SparkSession
      .builder()
      //dynamicAllocation.enabled
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // use this if you need to increment Kryo buffer size. Default 64k
      .config("spark.kryoserializer.buffer", "1024k")
      // use this if you need to increment Kryo buffer max size. Default 64m
      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.kryo.registrationRequired", "true")
      .config(getConfig)
      .appName(AppName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  private def getConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("org.apache.spark.unsafe.types.UTF8String"),
        Class.forName("[[B"),
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.ArrayType],
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        classOf[org.apache.spark.util.collection.BitSet],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        classOf[Array[Object]],
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
        Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        Class.forName("java.lang.Class"),
        Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"),
        //Class.forName("Object[]"),
        //自定义的类
        classOf[DataInit]
      )
    )
  }
}
