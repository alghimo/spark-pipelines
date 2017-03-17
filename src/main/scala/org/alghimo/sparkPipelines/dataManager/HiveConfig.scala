package org.alghimo.sparkPipelines.dataManager

import com.typesafe.config.ConfigFactory

/**
  * Created by alghimo on 10/30/2016.
  */
trait HiveConfig {
    def confResource = "hive"
    lazy val hiveConfig = ConfigFactory.load(confResource)
}
