package com.uaes.spark.analysis.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by hand on 2017/10/31.
  */
object TimeUtils {
  lazy val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def currentTime(args:Array[String]) : (String,String) = {
    val (startDate, endDate) = if (args.length < 2) {
      val cal: Calendar = Calendar.getInstance()
      val today = dateFormat.format(cal.getTime())
      cal.add(Calendar.DATE, -1)
      val yesterday = dateFormat.format(cal.getTime())
      (yesterday, today)
    } else {
      (args(0), args(1))
    }
    (startDate, endDate)
  }

}
