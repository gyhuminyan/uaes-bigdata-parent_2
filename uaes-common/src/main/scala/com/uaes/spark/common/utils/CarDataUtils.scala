package com.uaes.spark.common.utils

import com.alibaba.fastjson.JSON
import com.iot.common.inm.util.PayloadUtil

import scala.collection.mutable.ArrayBuffer


/**
  * Created by mzhang on 2017/10/20.
  */
object CarDataUtils {
  def transform(record :String):Array[String]={
    val jObj = JSON.parseObject(record) //将记录转为JSon对象
    val zipData = jObj.getString("DT") //获得压缩的数据
    val strJSons = analysis(zipData)  //解析CAN数据
    val resArr = new ArrayBuffer[String]()
    for(str <- strJSons){
      val jObjTmp = JSON.parseObject(str.toString)
      jObjTmp.put("timestamp",jObjTmp.remove("ts"))
      jObjTmp.put("VIN",jObj.get("FR"))
      resArr.append(jObjTmp.toString())
    }
    return resArr.toArray
  }

  private def analysis(data :String) : Iterator[_] ={
    import scala.collection.JavaConverters._
    PayloadUtil.parsePayload(data).asScala.iterator
  }
}
