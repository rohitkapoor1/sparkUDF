/**
 *  Using UDFs in Spark
 *  spark.udf
 *      org.apache.spark.sql.SparkSession.scala
 *          def udf: UDFRegistration = sessionState.udfRegistration
 *              * A collection of methods for registering user-defined functions (UDF).
 *
               * The following example registers a Scala closure as UDF:
               * {{{
               *   sparkSession.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
               * }}}
               *
 *
 *
 *  def register[RT: TypeTag, A1: TypeTag](name: String, func: Function1[A1, RT]): UserDefinedFunction =
 *      org.apache.spark.sql.UDFRegistration.scala
 *      Registers a deterministic Scala closure of 1 arguments as user-defined function (UDF).
 *
 *      calls -> functionRegistry.createOrReplaceTempFunction(name, builder)
 *
 * */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkUDF extends App {

  val runLocal = args(0).equals("l")
  val sparkConfig = new SparkConf()
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = if(runLocal) {
    sparkConfig.set("spark.master.bindAddress", "localhost")
    sparkConfig.set("spark.eventLog.enabled", "true")
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark_udf")
      .config(sparkConfig)
      .getOrCreate()
  }
  else {
    sparkConfig.set("spark.eventLog.enabled","true")
    SparkSession
      .builder()
      .appName("spark_udf")
      .config(sparkConfig)
      .getOrCreate()
  }
   def cubed = (s: Long) => { s * s * s}
  println(cubed(9))

  // query function registry for the UDF
  spark.udf.register("cubed",cubed)

  spark
    .sessionState
    .functionRegistry
    .listFunction()
    .filter(_.toString().startsWith("cube"))
    .foreach(println)

  // another way to register a UDF
  spark.udf.register("square", (arg1: Int) => arg1 * arg1)

  spark.range(1,9).createOrReplaceTempView("udf_test")
  // use the udf
  spark.sparkContext.setJobDescription("A: Spark SQL show")
  spark.sql("select id, cubed(id) as cubedID, square(id) as squared from udf_test").show()

  // stop spark
  println("enter any key to exit")
  System.in.read()
  spark.stop()
}