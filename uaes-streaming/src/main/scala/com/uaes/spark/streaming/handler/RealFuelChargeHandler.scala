package com.uaes.spark.streaming.handler

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.uaes.spark.streaming.service.HttpService
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mzhang on 2017/10/19.
  */

case class FuelRecords(var vin: String,
                       var list: ArrayBuffer[JSONObject],
                       var remindFlag: Boolean,
                       var realFuelCharge: Double,
                       var startTime: String,
                       var endTime: String,
                       var currentFuel: Double
                      )
  extends Serializable {
  def this(vin: String) {
    this(vin, ArrayBuffer(), false, 0.0, "", "", 0.0)
  }

}

class RealFuelChargeHandler(dStream: DStream[String]) extends BaseHandler(dStream) {
  def handle(): Unit = {
    //    dStream.transform(rdd => {
    //      val sqlSc = SQLContext.getOrCreate(rdd.sparkContext)
    //      val df = sqlSc.read.json(rdd)
    //      df.filter(f)
    //    })

    dStream.map(line => {
      val jObj = JSON.parseObject(line)
      jObj
    }).filter(jObj => jObj.get("stype").equals("fuelLevel") || jObj.get("stype").equals("drivingSpeed"))
      .transform(rdd => {
        rdd.map(jObj => {
          val vin = jObj.get("VIN").toString
          (vin, jObj)
        }).groupByKey().map(pair => {
          val list = pair._2.toList.sortWith(_.getString("time") < _.getString("time"))
          val listFuel = list.filter(jObj => jObj.get("stype").equals("fuelLevel"))
          val listSpeed = list.filter(jObj => jObj.get("stype").equals("drivingSpeed"))
          var sum = 0.0
          for (jObj <- listFuel) {
            sum += jObj.getDouble("value")
          }
          val tmp = listFuel.last
          var num = 0
          for (jObj <- listSpeed) {
            if (jObj.getDouble("value") <= 0) {
              num -= 1
            }
            else {
              num += 1
            }
          }
          if (num <= 0) {
            tmp.put("isDrive", true)
          }
          else {
            tmp.put("isDrive", false)
          }
          tmp.put("fuelLevel", sum / list.length)
          (pair._1, tmp)
        })
      })
      .updateStateByKey(updateFuelRecords)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val httpService = new HttpService()
          partition.foreach(pair => {
            if (pair._2.remindFlag) {
              //发送http消息
              sendToHttp(pair._2, httpService)
            }
          })
        })
      })
  }

  def updateFuelRecords(values: Seq[JSONObject],
                        state: Option[FuelRecords]) = {
    val currentJSonObj = values.head
    val vin = currentJSonObj.get("VIN").toString
    val currentFuel = currentJSonObj.getDouble("value")
    val oldRecord: FuelRecords = state.getOrElse(new FuelRecords(vin))
    oldRecord.realFuelCharge = 0.0
    oldRecord.remindFlag = false
    //oldRecord.list.append(currentJSonObj.getDouble("fuelLevel"))
    var lastFuel = oldRecord.list.last.getDouble("value")
    var firstFuel = oldRecord.list.head.getDouble("value")
    if (currentFuel - lastFuel >= 0) {
      oldRecord.list.append(currentJSonObj)
    } else {
      if (currentFuel - firstFuel < 0) {
        oldRecord.list.clear()
      }
      oldRecord.list.append(currentJSonObj)
    }

    val firstJObj = oldRecord.list.head
    val lastJObj = oldRecord.list.last

    firstFuel = firstJObj.getDouble("value")
    lastFuel = lastJObj.getDouble("value")


    if (lastFuel - firstFuel > 10) {
      //可能在加�?
      if (oldRecord.list.length > 4) {
        val lastFuel2 = oldRecord.list(oldRecord.list.length - 2).getDouble("value")
        val lastFuel3 = oldRecord.list(oldRecord.list.length - 3).getDouble("value")
        if (Math.abs(lastFuel - lastFuel2) < 1 && Math.abs(lastFuel2 - lastFuel3) < 1) {
          oldRecord.remindFlag = true
          oldRecord.realFuelCharge = lastFuel3 - firstFuel
          oldRecord.startTime = firstJObj.getString("timestamp")
          oldRecord.startTime = lastJObj.getString("timestamp")
          oldRecord.currentFuel = lastFuel3
          oldRecord.list.clear()
        }
      }
    }
    Some(oldRecord)
  }


  def sendToHttp(record: FuelRecords, httpService: HttpService): Unit = {
    val jObj = new JSONObject()
    jObj.put("msgcode", "fuelcharge")
    jObj.put("vin", record.vin)

    val arrJSon = new JSONArray()
    val attrJObj = new JSONObject()
    attrJObj.put("attributeType", "currentfuel")
    attrJObj.put("attributeValue", record.currentFuel)
    arrJSon.add(attrJObj)
    attrJObj.put("attributeType", "fuelcharge")
    attrJObj.put("attributeValue", record.realFuelCharge)
    arrJSon.add(attrJObj)
    attrJObj.put("attributeType", "startTime")
    attrJObj.put("attributeValue", record.startTime)
    arrJSon.add(attrJObj)
    attrJObj.put("attributeType", "endTime")
    attrJObj.put("attributeValue", record.endTime)
    arrJSon.add(attrJObj)
    jObj.put("attributeList", arrJSon)

    httpService.sendHttpRequest("", jObj.toString)
  }
}
