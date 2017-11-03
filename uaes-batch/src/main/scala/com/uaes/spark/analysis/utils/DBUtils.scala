package com.uaes.spark.analysis.utils

import com.uaes.spark.common.utils.JDBCWrapper
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Flink on 2017/10/24.
  */
object DBUtils {

  //将指标按每天每百公里的计算结果保存到MySql数据库
  def saveDayResultToDB(rdd: RDD[(String, String, String, Double)]): Unit ={
    JDBCWrapper.getInstance().truncate("t_car_used_day")
    rdd.foreachPartition(partition =>{
      val paramList = new ArrayBuffer[Array[Any]]()
      partition.foreach(tuple3 => {
        val carVin = tuple3._1
        val timestamp = tuple3._2
        val KPI = tuple3._3
        val avgVal = tuple3._4
        paramList += Array(carVin, timestamp, KPI, avgVal)
      })
      val sqlText = """insert into t_car_used_day (VIN, NOW_DATE, DATA_KEY, DATA_VALUE) values(?, ?, ?, ?)"""
      val result = JDBCWrapper.getInstance().doBatch(sqlText, paramList.toArray)
    })
  }

  //将指标按百公里计算结果保存到MySql数据库
  def saveMileageResultToDB(rdd: RDD[(String, Int, String, Double)]): Unit ={
    JDBCWrapper.getInstance().truncate("t_car_used_km")
    rdd.foreachPartition(partition =>{
      val paramList = new ArrayBuffer[Array[Any]]()
      partition.foreach(tuple3 => {
        val carVin = tuple3._1
        val mileage = tuple3._2
        val KPI = tuple3._3
        val avgVal = tuple3._4
        paramList += Array(carVin, mileage, KPI, avgVal)
      })
      val sqlText = """insert into t_car_used_km(VIN, MILEAGE, DATA_KEY, DATA_VALUE) values(?, ?, ?, ?)"""
      val result = JDBCWrapper.getInstance().doBatch(sqlText, paramList.toArray)
    })
  }

  //将指标按照两次加油时间间隔的计算结果保存到MySql数据库
  def saveKpiDurationToDB(rdd: RDD[(String, Int, String, String, String, Double)]): Unit = {
    JDBCWrapper.getInstance().truncate("uaes_iot_fuel_fill.t_fuel_fill_attr")
    rdd.foreachPartition(partition => {
      val paramList = new ArrayBuffer[Array[Any]]()
      partition.foreach(tuple6 => {
        val vin = tuple6._1
        val gasID = tuple6._2
        val fuelGrade = tuple6._3
        val startTime = tuple6._4
        val kpi = tuple6._5
        val avgVal = tuple6._6
        paramList += Array(vin, gasID, fuelGrade, startTime, kpi, avgVal)
      })
      val sqlText =
        """INSERT INTO uaes_iot_fuel_fill.t_fuel_fill_attr(VIN, GAS_ID, FILL_GRADE, FILL_DATE,
          |DATA_KEY, DATA_VALUE) VALUES(?, ?, ?, ?, ?, ?)""".stripMargin
      val result = JDBCWrapper.getInstance().doBatch(sqlText, paramList.toArray)
    })
  }
}