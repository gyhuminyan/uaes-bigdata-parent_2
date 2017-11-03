package com.uaes.spark.streaming.service

import java.net.URLEncoder
import java.nio.charset.Charset
import java.util

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpResponse, NameValuePair}
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicNameValuePair

/**
  * Created by mzhang on 2017/10/24.
  */
class HttpService {
  def sendHttpRequest(url: String, json: String): HttpResponse = {

    val client = new DefaultHttpClient
    val post = new HttpPost(url)

    post.addHeader("Content-type", "application/json; charset=utf-8");
    post.setHeader("Accept", "application/json");
    post.setEntity(new StringEntity(json, Charset.forName("UTF-8")));

    val response = client.execute(post)
    response
  }
}
