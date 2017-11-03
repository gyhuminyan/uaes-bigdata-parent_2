package com.uaes.spark.streaming.Launcher

import java.util.concurrent.atomic.AtomicReference

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.uaes.spark.common.utils.CarDataUtils
import com.uaes.spark.streaming.config.ConfigManager
import com.uaes.spark.streaming.handler.{LowFuelReminderEventHandler, RealFuelChargeHandler}
import com.uaes.spark.streaming.service.{KafkaOffsetService, ZkService}
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Created by mzhang on 2017/10/16.
  */
object Launcher {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(Launcher.getClass)
    //System.setProperty("hadoop.home.dir", "D:\\winutils\\winutils")
    var scc = null.asInstanceOf[StreamingContext]
    try {
      //      val conf = ConfigManager.getConfig()
      val conf = new SparkConf().setAppName("")
      "" match {
        case "local" => conf.setMaster("local[*]")
        case "yarn" => conf.setMaster("yarn-client")
        case _ => {
          logger.error("unknown spark run mode,system exit with code -1,please check your config file")
          System.exit(-1)
        }
      }

      conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      val ssc = new StreamingContext(conf, Seconds(5))
      ssc.checkpoint("E:\\\\Temp\\\\checkpoint")
      //ssc.addStreamingListener(new ConsumerListener(this, this.zkService))
      val offsetRanges = new AtomicReference[Array[OffsetRange]]()
      val offsetService = new KafkaOffsetService(offsetRanges, new ZkService("", 1234))
      val kafkaStream = offsetService.getStreamingContext(ssc)
      handleStream(kafkaStream)
      scc.start()
      scc.awaitTermination()
    } catch {
      case e: Exception => logger.error("failed to create streamingContext:", e)
    }
  }

  def handleStream(kafkaStream: DStream[(String, String)]): Unit = {
    val etledDStream = kafkaStream.map(pair => pair._2)
      .flatMap(record => {
        val jsArray = CarDataUtils.transform(record)
        val resArr = new ArrayBuffer[String]()
        val tmp = new JSONObject()
        val jObj = JSON.parseObject(record)
        val sendTime = jObj.getString("TS")

        for (i <- 0 to jsArray.size - 1) {
          val jObjTmp = JSON.parseObject(jsArray(i))
          val createTime = jObjTmp.getString("timestamp")
          if (sendTime.compare(createTime) > 0) {
            //比较发送时间和数据产生时间
            resArr.append(jsArray(i))
          }
        }
        resArr
      })

    //低油量提醒
    new Thread(new LowFuelReminderEventHandler(etledDStream)).start()
    //实际加油量
    new Thread(new RealFuelChargeHandler(etledDStream)).start()
  }
}
