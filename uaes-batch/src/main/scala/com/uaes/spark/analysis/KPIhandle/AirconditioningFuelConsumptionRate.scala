package com.uaes.spark.analysis.KPIhandle

import com.uaes.spark.analysis.utils.{AirconditioningFuelConsumptionUtil, SparkUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by mzhang on 2017/10/30.
  * 计算整体空调耗油率
  */

object AirconditioningFuelConsumptionRate {
  val logger = LoggerFactory.getLogger(AirconditioningFuelConsumptionRate.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext("AirconditioningFuelConsumptionRate",logger)

    val rdd = sc.textFile("hdfs://uaes/etl_data")
    val rate = AirconditioningFuelConsumptionUtil.ComputeFuelConsumptionRate(rdd)
    AirconditioningFuelConsumptionUtil.saveToDB(rate)
  }
}
