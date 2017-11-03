package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.JSON
import com.uaes.spark.analysis.KPIhandle.EconomicRanking.{getKey, saveToDB}
import com.uaes.spark.analysis.utils.{FuelFillUtil, SparkUtil}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hmy on 2017/10/27.
  * 计算平均驾驶速度
  */

object AverageDrivingSpeed {
  val logger = LoggerFactory.getLogger(AverageDrivingSpeed.getClass)

  def main(args: Array[String]) {
    val sc = SparkUtil.getSparkContext("AverageDrivingSpeed",logger)
    val rdd = sc.textFile("D:\\bigdata\\test.txt")

    val jsonRDD = rdd.filter(line => {
      val jObj = JSON.parseObject(line)
      jObj.get("stype").equals("drivingMileage") || jObj.get("stype").equals("rotationlSpeed")  //驾驶里程，发动机转速
    })
    val rddurationData = FuelFillUtil.getFuelFillRecordDurationData(jsonRDD,"rotationlSpeed")
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.json(rddurationData)
    df.createOrReplaceTempView("table1")

    //计算行驶里程
    val dfMileage = sqlContext.sql("select vin，gasID, startTime,fuelGrade, max(value)-min(value) " +
      "as mileage from table1 where stype='drivingMileage' group by vin，gasID, startTime,fuelGrade")
    val rddMileage = dfMileage.rdd.map(row => {
      val key = getKey(row)
      val mileage: Double = row.getAs("mileage")
      (key, mileage)
    })
    //计算转速总时间
    val dfTime = sqlContext.sql("select vin，gasID, startTime,fuelGrade, count(rotationlSpeed)*10 " +
      "as time from table1 where stype='rotationlSpeed' and rotationlSpeed > 0 group by vin，gasID, startTime,fuelGrade")
    val rddTime = dfTime.rdd.map(row => {
      val key = getKey(row)
      val time: Double = row.getAs("time")
      (key, time)
    })
    //两次加油间隔的行驶路程/转速的总时间
    val rddRes = rddMileage.join(rddTime).map(pair => {
      (pair._1, pair._2._1 / (pair._2._2 * 100 * 60 * 60))
    })
    saveToDB(rddRes)
  }
}
