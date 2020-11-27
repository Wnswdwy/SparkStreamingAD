package com.nswdwy.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author yycstart
 * @create 2020-11-25 11:37
 */
object PropertiesUtil {

  def load(propertiesName: String): Properties = {

    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }
}

