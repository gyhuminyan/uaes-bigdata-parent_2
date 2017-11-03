package com.uaes.spark.streaming.handler

import com.alibaba.fastjson.JSON
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class LowFuelReminderEventHandler(dStream: DStream[String]) extends BaseHandler(dStream) {
  def handle(): Unit = {

    val filtedDS = dStream.filter(line => {
      val jObj = JSON.parseObject(line)
      jObj.getString("stype").equals("fuelLevel")
    }).map(line => {
      val jObj = JSON.parseObject(line)
      val vin = jObj.getString("VIN")
      val value = jObj.getString("value").toDouble
      (vin, (value, 1))
    })

    filtedDS.reduceByKeyAndWindow((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }, (a, b) => {
      (a._1 - b._1, a._2 - b._2)
    }, Seconds(30), Seconds(10))
      .map(pair => {
        val avgFuel = pair._2._1 / pair._2._2
        (pair._1, avgFuel)
      })
      .foreachRDD(rdd => {
        rdd.foreach(pair => {
          if (pair._2 < 10) {
            //发送低油量提醒
            val vin = pair._1

          }
        })
      })
  }
}
