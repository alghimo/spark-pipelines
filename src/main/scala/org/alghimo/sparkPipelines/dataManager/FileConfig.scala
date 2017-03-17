package org.alghimo.sparkPipelines.dataManager

import com.typesafe.config.ConfigFactory

/**
  * Created by D-KR99TU on 21/02/2017.
  */
trait FileConfig {
  def confResource = "file"
  lazy val fileConfig = ConfigFactory.load(confResource)
}
