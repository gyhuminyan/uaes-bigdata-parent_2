package com.uaes.spark.streaming.handler

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by mzhang on 2017/10/20.
  */
abstract class BaseHandler[T](dStream :DStream[T]) extends Runnable{
  def run(): Unit ={
    handle()
  }
  def handle()
}
