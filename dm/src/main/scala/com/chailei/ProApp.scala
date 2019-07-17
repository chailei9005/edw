package com.chailei

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer


object ProApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ProApp")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SQLContext = new SQLContext(sc)

    val lines = spark.sparkContext.textFile("/Users/chailei/data/creditcard100")

//    lines.map(_.split("\t")(2)).map(JSON.parseArray)
//        .flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).foreach(println)

//    order_no	customerid	created_time	effective_date	loan_status	data

    val dataRDD = lines.flatMap(line => {
      val words = line.split("\t")
      val orderNo = words(0)
      val createdTime = words(2)
      val data = words(5)
      val arrayBuffer = ArrayBuffer[Row]()
      val dataArray = JSON.parseArray(data)
      for(index <- 0 until  dataArray.size()){
        val jsonObject = dataArray.getJSONObject(index)
        val balance = jsonObject.getDouble("balance")
        val bankId = jsonObject.getString("bank_id")
        val bankName = jsonObject.getString("bank_name")
        val bills = jsonObject.getString("bills")
        val cardId = jsonObject.getString("card_id")
        val cardNum = jsonObject.getString("card_num")
        val cardType = jsonObject.getString("card_type")
        val cashBalance = jsonObject.getDouble("cash_balance")

        val cashLimit = jsonObject.getDouble("cash_limit")
        val creditLimit = jsonObject.getDouble("credit_limit")
        val currentBillAmt = jsonObject.getDouble("current_bill_amt")
        val currentBillPaidAmt = jsonObject.getDouble("current_bill_paid_amt")
        val currentBillRemainAmt = jsonObject.getDouble("current_bill_remain_amt")
        val currentBillRemainMinPayment = jsonObject.getDouble("current_bill_remain_min_payment")

        val deposits = jsonObject.getString("deposits")
        val fullCardNum = jsonObject.getString("full_card_num")
        val nameOnCard = jsonObject.getString("name_on_card")
        val openBank = jsonObject.getString("open_bank")
        val openTime = jsonObject.getString("open_time")

        arrayBuffer.append(Row(orderNo, createdTime, balance, bankId, bankName, bills,
          cardId, cardNum, cardType, cashBalance, cashLimit, creditLimit,
          currentBillAmt, currentBillPaidAmt, currentBillRemainAmt, currentBillRemainMinPayment,
          deposits, fullCardNum, nameOnCard, openBank, openTime))
      }
      arrayBuffer.toArray[Row]
    })


    val datastructType = StructType(Array(StructField("order_no", StringType, true),
      StructField("created_time", StringType, true), StructField("balance", DoubleType, true),
      StructField("bankId", StringType, true), StructField("bankName", StringType, true),
      StructField("bills", StringType, true), StructField("cardId", StringType, true),
      StructField("cardNum", StringType, true), StructField("cardType", StringType, true),
      StructField("cashBalance", DoubleType, true), StructField("cashLimit", DoubleType, true),
      StructField("creditLimit", DoubleType, true), StructField("currentBillAmt", DoubleType, true),
      StructField("currentBillPaidAmt", DoubleType, true), StructField("currentBillRemainAmt", DoubleType, true),
      StructField("currentBillRemainMinPayment", DoubleType, true), StructField("deposits", StringType, true),
      StructField("fullCardNum", StringType, true), StructField("nameOnCard", StringType, true),
      StructField("openBank", StringType, true), StructField("openBank", StringType, true)))



    val baseInfo = spark.createDataFrame(dataRDD, datastructType)

    baseInfo.show()

    baseInfo.cache()


    val billsRDD = baseInfo.flatMap(line => {
      val bills = line.getAs[String]("bills")
      val arrayBuffer = ArrayBuffer[Row]()
      val billsArray = JSON.parseArray(bills)
      for (index <- 0 until billsArray.size()) {
        val billsObject = billsArray.getJSONObject(index)
        val adjust = billsObject.getDouble("adjust")
        val bankName = billsObject.getString("bank_name")
        val billDate = billsObject.getString("bill_date")
        val billId = billsObject.getString("bill_id")
        val billMonth = billsObject.getString("bill_month")
        val billType = billsObject.getString("bill_type")
        val cashLimit = billsObject.getDouble("cash_limit")
        val creditLimit = billsObject.getDouble("credit_limit")
        val installments = billsObject.getString("installments")
        val interest = billsObject.getDouble("interest")
        val lastBalance = billsObject.getDouble("last_balance")
        val lastPayment = billsObject.getDouble("last_payment")
        val minPayment = billsObject.getDouble("min_payment")
        val newBalance = billsObject.getDouble("new_balance")
        val newCharges = billsObject.getDouble("new_charges")
        val paymentDueDate = billsObject.getString("payment_due_date")
        val shoppingSheets = billsObject.getString("shopping_sheets")
        val usdCashLimit = billsObject.getDouble("usd_cash_limit")
        val usdCreditLimit = billsObject.getDouble("usd_credit_limit")
        val usdLastBalance = billsObject.getDouble("usd_last_balance")
        val usdLastPayment = billsObject.getDouble("usd_last_payment")
        val usdMinPayment = billsObject.getDouble("usd_min_payment")
        val usdNewBalance = billsObject.getDouble("usd_new_balance")
        val usdNewCharges = billsObject.getDouble("usd_new_charges")
        arrayBuffer.append(Row.merge(line, Row(adjust, bankName, billDate, billId, billMonth,
          billType, cashLimit, creditLimit, installments, interest, lastBalance, lastPayment,
          minPayment, newBalance, newCharges, paymentDueDate, shoppingSheets, usdCashLimit,
          usdCreditLimit, usdLastBalance, usdLastPayment, usdMinPayment, usdNewBalance, usdNewCharges)))
      }

      arrayBuffer.toArray[Row]

    })


    val billstructType = datastructType.add(StructField("adjust", DoubleType, true)).add(StructField("bank_name", StringType, true))
      .add(StructField("bill_date", StringType, true)).add(StructField("bill_id", StringType, true))
      .add(StructField("bill_month", StringType, true)).add(StructField("bill_type", StringType, true))
      .add(StructField("cash_limit", DoubleType, true)).add(StructField("credit_limit", DoubleType, true))
      .add(StructField("installments", StringType, true)).add(StructField("interest", DoubleType, true))
      .add(StructField("last_balance", DoubleType, true)).add(StructField("last_payment", DoubleType, true))
      .add(StructField("min_payment", DoubleType, true)).add(StructField("new_balance", DoubleType, true))
      .add(StructField("new_charges", DoubleType, true)).add(StructField("payment_due_date", StringType, true))
      .add(StructField("shopping_sheets", StringType, true)).add(StructField("usd_cash_limit", DoubleType, true))
      .add(StructField("usd_credit_limit", DoubleType, true)).add(StructField("usd_last_balance", DoubleType, true))
      .add(StructField("usd_last_payment", DoubleType, true)).add(StructField("usd_min_payment", DoubleType, true))
      .add(StructField("usd_new_balance", DoubleType, true)).add(StructField("usd_new_charges", DoubleType, true))

    val billDF = spark.createDataFrame(billsRDD, billstructType)


    billDF.show()

    billDF.cache()

    val installmentRDD = billDF.flatMap(line => {
      val installments = line.getAs[String]("installments")
      val arrayBuffer = ArrayBuffer[Row]()
      val installmentsArray = JSON.parseArray(installments)
      for (index <- 0 until installmentsArray.size) {
        val billsObject = installmentsArray.getJSONObject(index)
        val amountMoney = billsObject.getDouble("amount_money")
        val adjucurrencyTypest = billsObject.getString("currency_type")
        val currentMonth = billsObject.getLong("current_month")
        val handingFee = billsObject.getDouble("handing_fee")
        val handingfeeDesc = billsObject.getString("handingfee_desc")
        val installmentDesc = billsObject.getString("installment_desc")
        val installmentType = billsObject.getString("installment_type")
        val postDate = billsObject.getString("post_date")
        val shoppingsheetId = billsObject.getString("shoppingsheet_id")
        val totalMonth = billsObject.getLong("total_month")
        val transDate = billsObject.getString("trans_date")

        arrayBuffer.append(Row.merge(line, Row(amountMoney, adjucurrencyTypest, currentMonth, handingFee, handingfeeDesc, installmentDesc,
          installmentType, postDate, shoppingsheetId, totalMonth, transDate)))

      }
      arrayBuffer.toArray[Row]
    })


    val installmentStructType = billstructType.add(StructField("amount_money", DoubleType, true)).add(StructField("currency_type", StringType, true))
      .add(StructField("current_month", LongType, true)).add(StructField("handing_fee", DoubleType, true))
      .add(StructField("handingfee_desc", StringType, true)).add(StructField("installment_desc", StringType, true))
      .add(StructField("installment_type", StringType, true)).add(StructField("post_date", StringType, true))
      .add(StructField("shoppingsheet_id", StringType, true)).add(StructField("total_month", LongType, true))
      .add(StructField("trans_date", StringType, true))

    val installmentDF = spark.createDataFrame(installmentRDD, installmentStructType)

    installmentDF.show()

//    billstructType.printTreeString()

    val shoppingRDD = billDF.flatMap(line => {
      val shoppingSheets = line.getAs[String]("installments")
      val arrayBuffer = ArrayBuffer[Row]()
      val shoppingSheetsArray = JSON.parseArray(shoppingSheets)
      for (index <- 0 until shoppingSheetsArray.size) {
        val billsObject = shoppingSheetsArray.getJSONObject(index)
        val amountMoney = billsObject.getDouble("amount_money")
        val balance = billsObject.getDouble("balance")
        val card_num = billsObject.getString("card_num")
        val category = billsObject.getString("category")
        val currency_type = billsObject.getString("currency_type")
        val description = billsObject.getString("description")
        val id = billsObject.getString("id")
        val name_on_opposite_card = billsObject.getString("name_on_opposite_card")
        val opposite_bank = billsObject.getString("opposite_bank")
        val opposite_card_no = billsObject.getString("opposite_card_no")
        val order_index = billsObject.getLong("order_index")
        val post_date = billsObject.getString("post_date")
        val remark = billsObject.getString("remark")
        val trans_addr = billsObject.getString("trans_addr")
        val trans_channel = billsObject.getString("trans_channel")
        val trans_date = billsObject.getString("trans_date")
        val trans_method = billsObject.getString("trans_method")
        arrayBuffer.append(Row.merge(line, Row(amountMoney, balance, card_num, category, currency_type, description,
          id, name_on_opposite_card, opposite_bank, opposite_card_no, order_index, post_date, remark, trans_addr,
          trans_channel, trans_date, trans_method)))

      }

      arrayBuffer.toArray[Row]
    })


    val shoppingSheetsStructType = billstructType.add(StructField("shopping_amount_money", DoubleType, true)).add(StructField("shopping_balance", StringType, true))
      .add(StructField("shopping_card_num", StringType, true)).add(StructField("shopping_category", StringType, true))
      .add(StructField("shopping_currency_type", StringType, true)).add(StructField("shopping_description", StringType, true))
      .add(StructField("shopping_id", StringType, true)).add(StructField("shopping_name_on_opposite_card", StringType, true))
      .add(StructField("shopping_opposite_bank", StringType, true)).add(StructField("shopping_opposite_card_no", StringType, true))
      .add(StructField("shopping_order_index", LongType, true)).add(StructField("shopping_post_date", StringType, true))
      .add(StructField("shopping_remark", StringType, true)).add(StructField("shopping_trans_addr", StringType, true))
      .add(StructField("shopping_trans_channel", StringType, true)).add(StructField("shopping_trans_date", StringType, true))
      .add(StructField("shopping_trans_method", StringType, true))

    val shoppingSheetDF = spark.createDataFrame(shoppingRDD, shoppingSheetsStructType)

    shoppingSheetDF.show()



    val depositeRDD = baseInfo.flatMap(line => {
      val depositsSheets = line.getAs[String]("deposits")
      val arrayBuffer = ArrayBuffer[Row]()
      val depositsSheetsArray = JSON.parseArray(depositsSheets)

      depositsSheetsArray.size() match {
        case 0 =>{
          arrayBuffer.append(Row.merge(line,Row(9999d, 9999d ,9999d, "9999","9999","9999","9999","9999")))
        }
        case _ =>{
          for (index <- 0 until depositsSheetsArray.size) {
            val depositsObject = depositsSheetsArray.getJSONObject(index)
            val deperiod = depositsObject.getDouble("period")
            val depInterest = depositsObject.getDouble("interest")
            val depBalance = depositsObject.getDouble("balance")
            val depCurrencyType = depositsObject.getString("currency_type")
            val depositType = depositsObject.getString("deposit_type")
            val depositDate = depositsObject.getString("deposit_date")
            val depDueDate = depositsObject.getString("due_date")
            val deperiodUnit = depositsObject.getString("period_unit")
            arrayBuffer.append(Row.merge(line, Row(deperiod, depInterest, depBalance, depCurrencyType,
              depositType, depositDate, depDueDate, deperiodUnit)))
          }
        }
      }

      arrayBuffer.toArray[Row]
    })

    val depositStructType = datastructType.add(StructField("deposit_period", DoubleType, true)).add(StructField("deposit_interest", DoubleType, true))
      .add(StructField("deposit_balance", DoubleType, true)).add(StructField("deposit_currency_type", StringType, true))
      .add(StructField("deposit_type", StringType, true)).add(StructField("deposit_date", StringType, true))
      .add(StructField("deposit_due_date", StringType, true)).add(StructField("deposit_period_unit", StringType, true))




    val depositDF = spark.createDataFrame(depositeRDD, depositStructType)

    depositDF.show()




    sc.stop()


  }


}
