package com.chailei

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
/**
  * Created by chailei on 19/6/29.
  */
object TestApp {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParseJsonApp")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SQLContext = new SQLContext(sc)

    val lines = spark.read.format("json").load("/Users/chailei/data/20190707-test")

    lines.printSchema()

    lines.show()

    import spark.implicits._

    lines.filter(col("card_type") === "借记卡")
      .select(explode(col("bills")).as("bills")).select($"bills.*").show()

    lines.filter(col("card_type") === "借记卡")
      .select(col("bank_name"),col("bank_id"),col("card_type"),
        col("name_on_card"),col("card_id")).show(26,false)

//    lines.show()

    sc.stop()

  }

}
