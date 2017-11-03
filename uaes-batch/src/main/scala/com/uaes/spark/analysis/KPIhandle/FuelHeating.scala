package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.JSON
import com.uaes.spark.analysis.config.ConfigManager
import com.uaes.spark.analysis.utils.{DBUtils, FuelFillUtil, SparkUtil}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by Flink on 2017/10/24.
  */
object FuelHeating {

  lazy val url = ConfigManager.getConfig("mysql.url")
  lazy val username = ConfigManager.getConfig("mysql.user")
  lazy val password = ConfigManager.getConfig("mysql.password")
  lazy val table1 = "t_fuel_station"
  lazy val table2 = "t_fuel_fill_record"
  lazy val kpi = "燃油热值"


  def main(args: Array[String]) {
    val logger = LoggerFactory.getLogger(FuelHeating.getClass)
    val sc = SparkUtil.getSparkContext("FuelHeating", logger)
    // 读取HDFS上经过ETL处理过的一天的数据
    val carData = sc.textFile("hdfs://emr-header-1.cluster-50949:9000/uaes/etl_date/")
    val filterData = carData.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("fuelSupplyC-1")
    })
    val resRDD = FuelFillUtil.getFuelFillRecordDurationData(filterData, kpi)
    val sqlContext = SQLContext.getOrCreate(resRDD.sparkContext)
    val fuelHotDF = sqlContext.read.json(resRDD)
    fuelHotDF.createTempView("fuelHot")
    val avgValDF = sqlContext.sql("" +
      "SELECT VIN, GAS_ID, FUEL_GRADE, startTime," +
      "avg(1/value) AS avgVal FROM fuelHot" +
      "\nGROUP BY VIN, startTime, GAS_ID, FUEL_GRADE")
    val avgValRDD = avgValDF.rdd.map(row =>{
      val vin = row.getString(0)
      val gasID = row.getString(1)
      val fuelGrade = row.getString(2)
      val startTime = row.getString(3)
      val avgVal = row.getString(4).toDouble
      (vin, gasID, fuelGrade, startTime, kpi, avgVal)
    })

    //将燃油热值指标计算结果保存到MySql数据库
    DBUtils.saveKpiDurationToDB(avgValRDD)

  }
}
