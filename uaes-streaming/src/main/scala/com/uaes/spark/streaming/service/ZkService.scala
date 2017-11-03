package com.uaes.spark.streaming.service


import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.slf4j.LoggerFactory
import com.uaes.spark.streaming.utils.ZkUtils
/**
  * Created by mzhang on 2017/4/19.
  */
class ZkService(server: String, timeout: Int) extends Serializable {
  val logger = LoggerFactory.getLogger(classOf[ZkService])
  val client = ZkUtils.getZkClient(server, timeout)

  def createNode(path: String, data: String): Boolean = {
    try {
      if (client.exists(path)) {
        logger.warn("the path you created is exists")
      } else {
        val strs = path.split("/")
        var tmpPath = ""
        for(i <- 1 until  strs.length)
        {
          tmpPath += "/" + strs(i)
          if (!client.exists(tmpPath)) {
            client.create(tmpPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        }
      }
    } catch {
      case e :Exception=> logger.error("failed to create the zk node:", e)
    }
    true
  }

  def deleteNode(path: String): Boolean = {
    try {
      if (!client.exists(path)) {
        logger.warn("the node you delete does not exist")
      } else {
        client.delete(path)
      }
    } catch {
      case e: Exception => logger.error("failed to delete:", e)
    }
    true
  }

  def read(path: String): String = {
    client.readData[String](path, true)

  }

}



