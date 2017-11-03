package com.uaes.spark.analysis.KPIhandle

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, JSONObject}
import com.uaes.spark.analysis.KPIhandle.EconomicRanking.{getKey, saveToDB}
import com.uaes.spark.analysis.utils.{DBUtils, Per100Utils, SparkUtil}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hmy on 2017/10/27.
  * 计算每日、每百公里的累计耗油量
  */

object CumulativeFuelConsumption {
  val logger = LoggerFactory.getLogger(CumulativeFuelConsumption.getClass)
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext("CumulativeFuelConsumption",logger)
    val rdd = sc.textFile("D:\\bigdata\\test.txt")
        perDayCumulativeFuelConsumption(rdd,sc)
        per100CumulativeFuelConsumption(rdd,sc)
  }

  //每天的累计耗油量
  def perDayCumulativeFuelConsumption(rdd:RDD[String],sc:SparkContext): Unit ={
    //获取昨天的日期
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val yesterday = new SimpleDateFormat("yyyyMMdd").format(cal.getTime)

    val filtedRDD = rdd.filter(line => {
      val jObj = JSON.parseObject(line)
      jObj.get("stype").equals("InsFuelInjection")  //瞬时喷油量，每100毫秒传一次
    }).map(line => {
      val jObj = JSON.parseObject(line)
      val vin = jObj.getString("VIN")
      val data = jObj.getString("timestamp").substring(0,15)  //201710311332789
      jObj.put("timestamp",data)  //将时间转换一下，取出15位
      (vin + "_" + data,jObj)
    }).groupByKey().map(pair => {
      val tmpjObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpjObj.put("VIN",strs(0))
      tmpjObj.put("timestamp",strs(1))
      tmpjObj.put("InsFuelInjection",0)

      for (jObj <- pair._2) {
        if(jObj.get("stype").equals("InsFuelInjection")) {
          val value = jObj.getString("value").toInt
          tmpjObj.put("InsFuelInjection", value)
        }
      }
      tmpjObj.toJSONString
    })

    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
    val df = sqlContext.read.json(filtedRDD)
    df.createOrReplaceTempView("car")
    val ifInj = sqlContext.sql("select VIN,subString(timestamp,0,8),sum(InsFuelInjection) " +
      s"from car where subString(timestamp,0,8) = $yesterday group by VIN,subString(timestamp,0,8)")

    //保存到数据库
    val rddFuel = ifInj.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val kpi = "perDayCumulativeFuelConsumption"
      val totalFuel:Double = row.getString(2).toDouble
      (vin, date,kpi,totalFuel)
    })
    DBUtils.saveResultToDB(rddFuel)

  }
  //每百公里耗油量
  def per100CumulativeFuelConsumption(rdd:RDD[String],sc:SparkContext): Unit ={

    val filtedRDD = rdd.filter(line => {
      val jObj = JSON.parseObject(line)
      jObj.get("stype").equals("InsFuelInjection") || jObj.get("stype").equals("drivingMileage")
    })
    val per100UtilsRdd = Per100Utils.getAfterLastPer100Data(filtedRDD, "每百公里累计耗油量")

    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.json(per100UtilsRdd)
    df.createOrReplaceTempView("table1")

    //计算耗油
    val dfFuel = sqlContext.sql("select vin，gasID, startTime,fuelGrade, sum(value*100) as totalFuel " +
      "as mileage from table1 where stype='InsFuelInjection' group by vin，gasID, startTime,fuelGrade")

    val rddFuel = dfFuel.rdd.map(row => {
      val key = getKey(row)
      val vin = row.getString(0)
      val date = row.getString(1)
      val kpi = "per100CumulativeFuelConsumption"
      val totalFuel: Double = row.getAs("totalFuel")
      (vin,date,kpi,totalFuel)
    })
    DBUtils.saveResultToDB(rddFuel)
  }
}
