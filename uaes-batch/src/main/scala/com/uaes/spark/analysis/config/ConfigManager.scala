package com.uaes.spark.analysis.config

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConverters._

/**
  * Created by mzhang on 2017/10/16.
  */
object ConfigManager {
  lazy val conf = ConfigFactory.load("app.conf")
  def getConfig(key:String):String={
    key
  }
  def getConfig():Unit = {
    val appName = conf.getString("spark.app_name")
    val runMode = conf.getString("spark.run_mode")
    val maxBath = conf.getLong("spark.batch_num")
    val batchDuration = conf.getLong("spark.batch_duration")
    val checkpointPath = conf.getString("spark.checkpoint_path")
    val masterUrl= conf.getString("spark.master")

    val brokerList = conf.getStringList("kafka.broker_list").asScala.toList
    val kafkaPort = conf.getInt("kafka.port")
    val topicList = conf.getStringList("kafka.topic").asScala.toList
    val groupId = conf.getString("kafka.consumer.group_id")
    val offsetReset = conf.getString("kafka.consumer.auto_offset_reset")
    val kafkaServerString = brokerList.map(f => f + ":" + kafkaPort).addString(new StringBuilder(), ",").toString()

    val zkConnectionList = conf.getStringList("zookeeper.zkConnect").asScala.toList
    val zkPort = conf.getInt("zookeeper.port")
    var zkServerString = zkConnectionList.map(f => f + ":" + zkPort).addString(new StringBuilder(), ",")
    val zkNodePath = conf.getString("zookeeper.node_path")
    if (StringUtils.isNotEmpty(zkNodePath)) {
      zkServerString = zkServerString.append(zkNodePath)
    }
    val zkTimeout = conf.getLong("zookeeper.timeout")

    val hdfsUser = conf.getString("hdfs.user")
    val hdfsRootPath = conf.getString("hdfs.root_path")

    val mysqlUrl = conf.getString("mysql.url")
    val mysqlUser = conf.getString("mysql.user")
    val mysqlPassword = conf.getString("mysql.password")
  }

}
