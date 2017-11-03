package com.uaes.spark.common.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.LinkedBlockingDeque

/**
  * Created by mzhang on 2017/3/29.
  */
object JDBCWrapper {
  private var jdbcInstance: JDBCWrapper = _

  def getInstance(): JDBCWrapper = {
    synchronized {
      if (jdbcInstance == null) {
        jdbcInstance = new JDBCWrapper()
      }
    }
    jdbcInstance
  }
}

class JDBCWrapper {
  private val dbConnectionPool = new LinkedBlockingDeque[Connection]()
  Class.forName("com.mysql.jdbc.Driver")
  val url = "jdbc:mysql://132.176.22.169:13306/isa?characterEncoding=utf8"
  val user = "isa"
  val password = "isa1qaz2wsx"
//  val mysqlConf = ConfigManager.getConfig().mysqlConf
  for (i <- 1 to 3) {
    val conn = DriverManager.getConnection(url, user, password)
    dbConnectionPool.put(conn)
  }

  private def getConnection(): Connection = synchronized {
    while (dbConnectionPool.size() == 0) {
      Thread.sleep(20);
    }
    dbConnectionPool.poll()
  }

  def execute(sqlText: String, paramsList: Array[_]): Boolean = {
    val conn = getConnection()
    var preparedStatement: PreparedStatement = null;
    try {
      preparedStatement = conn.prepareStatement(sqlText)
      if (paramsList != null) {
        for (i <- 0 to paramsList.length - 1) {
          preparedStatement.setObject(i + 1, paramsList(i))
        }
      }
      preparedStatement.execute()
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if(conn != null){
        dbConnectionPool.put(conn)
      }
    }
  }

  def doBatch(sqlText: String, paramsList: Array[Array[_]]): Array[Int] = {
    val conn = getConnection()
    var preparedStatement: PreparedStatement = null;
    try {
      conn.setAutoCommit(false);
      preparedStatement = conn.prepareStatement(sqlText)
      if (paramsList != null) {
        for (i <- 0 to paramsList.length - 1) {
          val list = paramsList(i)
          if (list != null) {
            for (j <- 0 to list.length - 1) {
              preparedStatement.setObject(j + 1, list(j))
            }
            preparedStatement.addBatch()
          }

        }
      }
      val result = preparedStatement.executeBatch();
      conn.commit()
      return result
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if(conn != null){
        dbConnectionPool.put(conn)
      }
    }
  }

  def query(sqlText: String, paramsList: Array[_], callback: ResultSet => Unit): Unit = {
    val conn = getConnection()
    var preparedStatement: PreparedStatement = null;
    try {
      preparedStatement = conn.prepareStatement(sqlText)
      if (paramsList != null) {
        for (i <- 0 to paramsList.length - 1) {
          preparedStatement.setObject(i + 1, paramsList(i))
        }
      }
      val rs = preparedStatement.executeQuery() //执行查询
      callback(rs)
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      dbConnectionPool.put(conn)
    }
  }
  def truncate(tableName:String): Unit ={
    val sql = s"truncate table $tableName"
    execute(sql,null)
  }
  override def finalize(): Unit = {
    super.finalize()
    println("call finalize")
    while (dbConnectionPool.size() > 0) {
      dbConnectionPool.poll().close();
    }
  }
}
