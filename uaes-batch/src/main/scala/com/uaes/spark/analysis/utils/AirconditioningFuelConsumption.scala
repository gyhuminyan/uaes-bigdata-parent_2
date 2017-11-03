package com.uaes.spark.analysis.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by mzhang on 2017/10/30.
  */
object AirconditioningFuelConsumptionUtil {
  def ComputeFuelConsumptionRate(rdd: RDD[String]): Double = {
    val rddFilted = rdd.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("InsFuelInjection") || jobj.get("stype").equals("airConditioningState")
    })

//    val totalFue = rddFilted.filter(line => {
//      val jObj = JSON.parseObject(line)
//      jObj.get("stype").equals("InsFuelInjection")
//    }).map(line => {
//      val jobj = JSON.parseObject(line)
//      jobj.get("value").toString.toInt
//    }).sum()

    val rddRes = rddFilted.map(line => {
      val jObj = JSON.parseObject(line)
      val vin = jObj.get("vin").toString
      val timestamp = jObj.get("timestamp").toString
      (vin + "_" + timestamp, jObj)
    }).groupByKey().map(pair => {
      var airState = 0
      var fuelSum = 0D
      var duration = 0L
      for (jObj <- pair._2) {
        val stype = jObj.get("stype").toString
        stype match {
          case "InsFuelInjection" => {
            fuelSum += jObj.getDouble("value")
            duration += 100L
          }
          case "airConditioningState" => {
            airState += (if (jObj.getInteger("value") == 0) -1 else 1)
          }
        }
      }
      airState = if( airState< 0) 0 else 1
      (airState, (duration,fuelSum))
    }).reduceByKey((a,b) => {(a._1 + b._1,a._2 + b._2)})

    val arr = rddRes.collect()
    val totalFuel = arr(0)._2._2 + arr(1)._2._2
    val onPair = if( arr(0)._1 == 1) arr(0) else arr(1)
    val offPair = if( arr(0)._1 == 1) arr(1) else arr(0)
    val onTimeTotal = onPair._2._2/3600000D
    val onPerHour = onPair._2._2 / onTimeTotal
    val offPerHour = onPair._2._2 / (onPair._2._2/3600000D)

    (onPerHour - offPerHour)* onTimeTotal / totalFuel
  }

  def saveToDB(rate :Double):Unit ={

  }

  def getRateFromDB() : Double = {

    0
  }
}
