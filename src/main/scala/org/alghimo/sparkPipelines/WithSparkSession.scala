package org.alghimo.sparkPipelines

import org.apache.spark.sql.SparkSession

/**
  * Used to mix in the Spark Session.
  */
trait WithSparkSession {
    @transient def spark: SparkSession
}
