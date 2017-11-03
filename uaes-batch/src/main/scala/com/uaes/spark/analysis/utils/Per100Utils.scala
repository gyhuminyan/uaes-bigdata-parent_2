package com.uaes.spark.analysis.utils

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by mzhang on 2017/10/27.
  */
object Per100Utils {
  def getLastPer100AsDataFrame(sc: SparkContext, stype: String): DataFrame = {
    //从数据库中得到车辆最后一次记录的百公里节点
    val sqlc = SQLContext.getOrCreate(sc)
    val properties = new Properties()
    val df = sqlc.read.jdbc("", "", properties)
    df.createTempView("table1")
    val sqlStr = s"select vin,max(per100)*100 as per100 from table1 where stype='$stype' group by vin"
    val lastDF = sqlc.sql(sqlStr)
    lastDF
  }

  def getLastPer100AsRDD(sc: SparkContext, stype: String): RDD[(String, Int)] = {
    //从数据库中得到车辆最后一次记录的百公里节点
    val lastDF = getLastPer100AsDataFrame(sc, stype)
    lastDF.rdd.map(row => {
      (row.getAs("vin").toString, row.getAs("per100").toString.toInt)
    })
  }

  /**
    *
    * @param rdd : 车辆所有数据
    * @param stype ：要计算的指标
    * @return 需要计算的每百公里的数据
    */
  def getAfterLastPer100Data(rdd: RDD[String], stype: String): RDD[String] = {
    val dfLast = getLastPer100AsDataFrame(rdd.sparkContext, stype)
    dfLast.createTempView("lastTbl")

    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)

    val dfFull = sqlContext.read.json(rdd)
    dfFull.createTempView("fullTbl")

    // 得到仅包含里程数的rdd
    val dfMileage = sqlContext.sql("select vin , timestamp, FLOOR(value/100) as value from fullTbl where stype='drivingMileage'")
    dfMileage.createTempView("mileageTbl")

    //
    val dfMileageTime = sqlContext.sql("select vin as vin1, value as mileage,min(timestamp) as timestamp1 from mileageTbl group by vin, value")
    dfMileageTime.createTempView("mileageTimeTbl")


    //    dfMileageTime.join(lastFD, dfMileageTime("vin1") === lastFD("vin"), "left").createTempView("joinedTbl")
    //    val dfJoined = sqlContext.sql("select vin1,timestamp,mileage, nvl(per100,0) as per100 from joinedTbl")
    //
    //    val dfFilted = dfJoined.filter(dfJoined("mileage") >= dfJoined("per100")).
    //      select("vin1","timestamp").withColumnRenamed("timestamp","timestamp1")
    val dfJoined = sqlContext.sql("select vin1, timestamp1 from mileageTimeTbl left join lastTbl on vin=vin1 where mileage>=per100")
    dfJoined.createTempView("joinedTble")

    val dfTime = sqlContext.sql("select vin1, min(timestamp1) as timestamp1 from joinedTble group by vin1" )
    dfTime.createTempView("timeTbl")

    val dfRes = sqlContext.sql("select vin,timestamp,stype,value from fullTbl inner jion timeTbl on vin=vin1 where timestamp>=timestamp1")

    dfRes.toJSON.rdd
  }
}
