package org.alghimo.sparkPipelines.dataManager

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by alghimo on 10/30/2016.
  */
trait HiveConfig {
  def options: Map[String, String]
  def configFile= {
    if (options.isDefinedAt("hive_config")) {
      new File(options("hive_config"))
    } else {
      val resourceName = options.getOrElse("hive_resource_name", "/hive.conf")
      val resource = getClass.getResource(resourceName).getFile
      new File(resource)
    }
  }

  lazy val hiveConfig = ConfigFactory.parseFile(configFile)
}
