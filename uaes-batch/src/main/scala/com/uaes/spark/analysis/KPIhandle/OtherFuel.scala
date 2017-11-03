package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.uaes.spark.analysis.utils.{DBUtils, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hand on 2017/10/29.
  */
object OtherFuel {
  val logger = LoggerFactory.getLogger(OtherFuel.getClass)


  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext("WaitSpeedFuel", logger)
    val rdd = sc.textFile("H:/UAES/TestData.txt")
    everyDayOtherFuel(rdd)
    everyHunKiOtherFuel(rdd)
  }

  //  每天其他耗油
  def everyDayOtherFuel(rdd: RDD[String]): Unit = {
    val jsonRDD = rdd.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("InsFuelInjection")
    }).map(line => {
      val jobj = JSON.parseObject(line)
      val date = jobj.getString("timestamp").substring(8)
      val vin = jobj.getString("VIN")
      jobj.put("timestamp", date)
      (vin + "_" + date, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("timestamp", strs(1))
      tmpObj.put("InsFuelInjection", 0)
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "InsFuelInjection" => {
            val value = jObj.getString("value").toInt
            tmpObj.put("InsFuelInjection", value)
          }
        }
      }
      tmpObj.toJSONString
    })

    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
    val df = sqlContext.read.json(jsonRDD)
    df.createTempView("car")

    val resDF = sqlContext.sql("select VIN,subString(timestamp,0,8) as date,sum(InsFuelInjection) as totalFuel " +
      "from car group by VIN,subString(timestamp,0,8)")

    //保存到数据库
    val resRDD = resDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val KPI = row.getString(2)
      val totalFuel = row.getString(3).toDouble
      (date, vin, KPI, totalFuel)
    })
    DBUtils.saveResultToDB(resRDD)
  }

  //每百公里其他耗油
  def everyHunKiOtherFuel(rDD: RDD[String]): Unit = {
    val jsonRDD = rDD.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("InsFuelInjection") //瞬时喷油量
    }).map(line => {
      val jobj = JSON.parseObject(line)
      val vin = jobj.getString("VIN")
      val drivingMileage = (jobj.getString("drivingMileage").toDouble / 100).toInt * 100
      val date = jobj.getString("timestamp").substring(14)
      jobj.put("timestamp", date)
      (vin + "_" + drivingMileage, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("everyHunKil", strs(1))
      tmpObj.put("InsFuelInjection", 0)
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "InsFuelInjection" => {
            val value = jObj.getString("value").toInt
            tmpObj.put("InsFuelInjection", value)
          }
        }
      }
      tmpObj.toJSONString
    })
    val sqlContext = SQLContext.getOrCreate(rDD.sparkContext)
    val df = sqlContext.read.json(jsonRDD)
    df.createTempView("car")

    val resDF = sqlContext.sql("select VIN,everyHunKil,KPI,sum(InsFuelInjection) as totalFuel " +
      "from car group by VIN,everyHunKil")
    //保存到数据库
    val resRDD = resDF.rdd.map(row => {
      val vin = row.getString(0)
      val everyHunKil = row.getString(1)
      val KPI = row.getString(2)
      val totalFuel = row.getString(3).toDouble
      (everyHunKil, vin, KPI, totalFuel)
    })
    DBUtils.saveResultToDB(resRDD)
  }
}
