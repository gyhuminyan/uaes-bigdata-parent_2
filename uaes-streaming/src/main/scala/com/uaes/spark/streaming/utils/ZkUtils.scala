package com.uaes.spark.streaming.utils

import java.util.{Calendar, Date}

import org.I0Itec.zkclient.ZkClient

/**
  * Created by zgl on 2017/4/19.
  */
object ZkUtils {
  private val date = new Date()

  def getZkClient(server: String, timeout: Int): ZkClient = {
    new ZkClient(server, timeout)
  }

  def getFlagPath(): List[String] = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val ts = System.currentTimeMillis()
    List("/" + year, "/" + year + "/" + month, "/" + year + "/" + month + "/" + day, "/" + year + "/" + month + "/" + day + "/" + ts)
  }
}

