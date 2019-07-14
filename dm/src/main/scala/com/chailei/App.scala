package com.chailei

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

object App {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParseJsonApp")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SQLContext = new SQLContext(sc)

    import spark.implicits._

    val jsonString =Seq(
      """
        |{
        |	"key": [{
        |		"name": "chailei",
        |		"age": 18,
        |		"hobby": {
        |			"first": "footbal",
        |			"second": "balll"
        |		}
        |	}, {
        |		"name": "baixue",
        |		"age": 28,
        |		"hobby": {
        |			"first": "paly",
        |			"second": "sleep"
        |		}
        |	}]
        |}
      """).toDS().rdd

    val schema = new StructType().add("key",new ArrayType(new StructType().add("name", StringType)
      .add("age", IntegerType).add("hobby", MapType(StringType, new StructType()
      .add("first", StringType).add("second", StringType))), true))


    val lines = spark.read.schema(schema).json(jsonString)
    lines.printSchema()

    lines.select("key").show()

    val jsonString1 =Seq(
      """
        |{
        |	"key": {
        |		"name": "chailei",
        |		"age": 18,
        |		"hobby": {
        |			"first": "footbal",
        |			"second": "balll"
        |		}
        |	}
        |}
      """).toDS().rdd

    val schema1 = new StructType().add("key", MapType(StringType,new StructType().add("name", StringType)
      .add("age", IntegerType).add("hobby", MapType(StringType, new StructType()
      .add("first", StringType).add("second", StringType))), true))

    spark.read.schema(schema1).json(jsonString1).show()

    sc.stop()
  }

}
