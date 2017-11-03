package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.JSON
import com.uaes.spark.analysis.utils.{FuelFillUtil, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Created by mzhang on 2017/11/1.
  *
  * 经济性排名
  *
  */
object EconomicRanking {
  val logger = LoggerFactory.getLogger(EconomicRanking.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext("EconomicRanking", logger)
    val rdd = sc.textFile("hdfs://")
    val rddFilted = rdd.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("InsFuelInjection") || jobj.get("stype").equals("drivingMileage")
    })

    val rddurationData = FuelFillUtil.getFuelFillRecordDurationData(rddFilted, "经济性排名")
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.json(rddurationData)
    df.createTempView("table1")

    //计算行驶里程
    val dfMileage = sqlContext.sql("select vin，gasID, startTime,fuelGrade, max(value)-min(value) " +
      "as mileage from table1 where stype='drivingMileage' group by vin，gasID, startTime,fuelGrade")
    val rddMileage = dfMileage.rdd.map(row => {
      val key = getKey(row)
      val mileage: Double = row.getAs("mileage")
      (key, mileage)
    })

    //计算耗油
    val dfFuel = sqlContext.sql("select vin，gasID, startTime,fuelGrade, sum(value*100) as totalFuel " +
      "as mileage from table1 where stype='InsFuelInjection' group by vin，gasID, startTime,fuelGrade")
    val rddFuel = dfFuel.rdd.map(row => {
      val key = getKey(row)
      val totalFuel: Double = row.getAs("totalFuel")
      (key, totalFuel)
    })

    //得到油价
    val dfPrice = FuelFillUtil.getFuelPrice(sc)
    val rddPrice = dfPrice.rdd.map(row => {
      val key = getKey(row)
      val price: Double = row.getAs("price")
      (key, price)
    })

    val rddRes = rddFuel.join(rddMileage).map(pair => {
      (pair._1, pair._2._1 / pair._2._2)
    }).join(rddPrice).map(pair => {
      (pair._1, pair._2._1 * pair._2._2)
    })

    saveToDB(rddRes)
  }

  def getKey(row: Row): String = {
    val vin: String = row.getAs("vin")
    val gasID: String = row.getAs("gasID")
    val startTime: String = row.getAs("startTime")
    val fuelGrade: String = row.getAs("fuelGrade")
    (vin + "_" + gasID + "_" + startTime + "_" + fuelGrade)
  }

  def saveToDB(rdd: RDD[(String, Double)]): Unit = {
    rdd.foreachPartition(partition => {
      partition.foreach(pair => {
        val strs = pair._1.split("_")
        val ranking =  pair._2
      })
    })
  }
}
