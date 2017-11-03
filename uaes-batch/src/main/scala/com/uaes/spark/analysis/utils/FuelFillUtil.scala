package com.uaes.spark.analysis.utils

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.uaes.spark.analysis.config.ConfigManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mzhang on 2017/11/1.
  */
object FuelFillUtil {

  lazy val url = ConfigManager.getConfig("mysql.url")
  lazy val username = ConfigManager.getConfig("mysql.user")
  lazy val password = ConfigManager.getConfig("mysql.password")
  lazy val table1 = "t_fuel_station"
  lazy val table2 = "t_fuel_fill_record"

  def getNewFuelFillRecord(sc: SparkContext, kpi: String): RDD[(String, JSONObject)] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val prop = new Properties()
    prop.put("user", username)
    prop.put("password", password)
    val tfsDF = sqlContext.read.jdbc(url, table1, prop)
    val tffrDF = sqlContext.read.jdbc(url, table2, prop)
    tfsDF.createTempView("t_fuel_station")
    tffrDF.createTempView("t_fuel_fill_record")
    val fuelDF = sqlContext.sql("SELECT VIN, FILL_START_TIME as startTime, GAS_ID," +
      " NVL(FILL_TS, '1753-00-00 00:00:00') as Fill_TS " +
      "\nFROM t_fuel_fill_record " +
      "\nLEFT JOIN" +
      "\n(SELECT VIN AS VIN1, MAX(FILL_TS) AS FILL_TS " +
      "\nFROM t_fuel_station GROUP BY VIN) AS a" +
      "\nON VIN = VIN1" +
      "\nWHERE FILL_START_TIME > NVL(FILL_TS, '1753-00-00 00:00:00')")

    //    fuelDF.show()
    val fuelRDD = fuelDF.toJSON.rdd.map(line => {
      val jobj = JSON.parseObject(line)
      val vin = jobj.getString("VIN")
      (vin, line)
    }).groupByKey()
      .filter { field => field._2.size > 1 }
      .flatMap(pair => {
        val list = pair._2.toList
        val arr = new ArrayBuffer[(String, JSONObject)]()
        for (index <- 0 to list.length - 2) {
          val jObj1 = JSON.parseObject(list(index))
          val jObj2 = JSON.parseObject(list(index + 1))
          jObj1.put("endTime", jObj2.get("startTime"))
          arr.append((pair._1, jObj1))
        }
        arr
      })
    fuelRDD
  }

  /**
    *
    * @param rdd
    * @param kpi
    * @return json字符串的rdd。json包含vin，timestamp, stpey,value, gasID(加油站id),
    *         startTime(加油时间),fuelGrade(油号)
    */
  def getFuelFillRecordDurationData(rdd: RDD[String], kpi: String): RDD[String] = {
    val rddFuelFillDuration = getNewFuelFillRecord(rdd.sparkContext, kpi)
    val rddPair = rdd.map(line => {
      val jobj = JSON.parseObject(line)
      val vin = jobj.getString("VIN")
      (vin, jobj)
    })
    val rddJoined = rddPair.join(rddFuelFillDuration)
    val rddRes = rddJoined.filter(pair => {
      val jObj1 = pair._2._1
      val jObj2 = pair._2._2
      val timestamp = jObj1.getString("timestamp")
      val startTime = jObj2.getString("startTime")
      val endTime = jObj2.getString("endTime")
      startTime <= timestamp  && timestamp < endTime
    }).map(pair =>{
      val jObj1 = pair._2._1
      val jObj2 = pair._2._2
      val startTime = jObj2.getString("startTime")
      val gasId = jObj2.getString("gasId")
      val fuelGrade = jObj2.getString("fuelGrade")
      jObj1.put("gasId",gasId)
      jObj1.put("startTime",startTime)
      jObj1.put("fuelGrade",fuelGrade)
      jObj1.toString
    })
    rddRes
  }

  def getFuelPrice(sc :SparkContext ) : DataFrame={
    val sqlContext = SQLContext.getOrCreate(sc)
    val prop = new Properties()
    prop.put("user", username)
    prop.put("password", password)
    val tfsDF = sqlContext.read.jdbc(url, table2, prop)
    tfsDF.createTempView("table")
    sqlContext.sql("select vin,gasId,startTime,gasGrade,price from table1")
  }

}
