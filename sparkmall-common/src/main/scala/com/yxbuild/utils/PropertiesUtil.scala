package com.yxbuild.utils

import java.io.InputStreamReader
import java.util.Properties


/**
  * 用来读取配置文件的工具类
  */
object PropertiesUtil {

  /**
    * 读取Property配置文件内容
    *
    * @param propertiesName 文件名
    * @return
    */
  def load(propertiesName:String): Properties  = {
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName) , "UTF-8"))
    properties
  }
}
