package com.chailei

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
/**
  * Created by chailei on 19/7/6.
  */
object ParseJsonApp {

  def main(args: Array[String]) {


    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParseJsonApp")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SQLContext = new SQLContext(sc)


    val lines: DataFrame = spark.read.format("json").load("/Users/chailei/data/creditcard1.txt")

    import spark.implicits._

    lines.select(explode(col("bills")).as("bills")).select($"bills.*").show()
    lines.show()





  }

}
