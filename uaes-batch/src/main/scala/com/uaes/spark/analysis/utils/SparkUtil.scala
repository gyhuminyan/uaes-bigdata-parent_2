package com.uaes.spark.analysis.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by mzhang on 2017/10/30.
  */
object SparkUtil {
    def getSparkContext(appName : String, logger : Logger ) : SparkContext ={
      val conf = new SparkConf().setAppName(appName)
      val run_mode = "local"
      run_mode match {
        case "local" => conf.setMaster("local[*]")
        case "yarn" => conf.setMaster("yarn-client")
        case _ => {
          logger.error("unknown spark run mode,system exit with code -1,please check your config file")
          System.exit(-1)
        }
      }

      val sc = new SparkContext(conf)
      sc
    }
}
