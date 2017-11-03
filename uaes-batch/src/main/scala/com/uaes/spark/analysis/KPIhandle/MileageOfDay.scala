package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.JSON
import com.uaes.spark.analysis.config.KPIConfig
import com.uaes.spark.analysis.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Created by mzhang on 2017/11/1.
  *
  * 每日里程数
  */
object MileageOfDay {
  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(MileageOfDay.getClass)

    val sc = SparkUtil.getSparkContext("EconomicRanking", logger)
    val rdd = sc.textFile("hdfs://")
    val (startTime, endTime) = ("", "")
    val rddFilted = rdd.filter(line => {
      val jobj = JSON.parseObject(line)
      val timestamp = jobj.getString("timestamp")
      jobj.get("stype").equals("drivingMileage") || (
        startTime <= timestamp && timestamp < endTime)
    }).map(line => {
      val jobj = JSON.parseObject(line)
      val timestamp = jobj.getString("timestamp")
      jobj.put("timestamp", timestamp.substring(0, 9))
      jobj.toString
    })
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.json(rddFilted)
    df.createTempView("table1")

    //计算行驶里程
    val dfMileage = sqlContext.sql("select vin，timestamp, max(value)-min(value) " +
      "as mileage from table1 group by vin timestamp")
    saveToDB(dfMileage)
  }

  def saveToDB(df: DataFrame): Unit = {

  }
}
