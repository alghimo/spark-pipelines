package org.alghimo.sparkPipelines.dataManager

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Created by D-KR99TU on 21/02/2017.
  */
trait FileConfig {
  def options: Map[String, String]
  def configFile = {
    if (options.isDefinedAt("file_config")) {
      new File(options("file_config"))
    } else {
      val resource = getClass.getResource("/file.conf").getFile
      new File(resource)
    }
  }

  lazy val fileConfig = ConfigFactory.parseFile(configFile)
}
