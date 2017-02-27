package org.alghimo.sparkPipelines

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/**
  * Concrete implementation of a DataManager that handles reading and writing to Hive.
  * @param spark
  * @param confResource Set to "hive" by default. You can point it to wherever you have your config file.
  */
class HiveDataManager(@transient val spark: org.apache.spark.sql.SparkSession, override val confResource: String = "hive") extends DataManager with HiveConfig {
  def hasResource(key: String): Boolean = {
    hiveConfig.hasPath(s"tables.${key}")
  }

  def resourceName(key: String): String = {
        val db      = hiveConfig.getString(s"tables.${key}.db")
        val table   = hiveConfig.getString(s"tables.${key}.table")
        s"${db}.${table}"
    }

    /**
      * Get a DataFrame from a Hive table. This will look for the keys inside the "tables" config element.
      * For every key, you must specify at least "db" and "table".
      * Apart from that, you can define:
      * - "select": List of string expressions. You can use it to project columns (rename, modify, etc)
      * - "filter": Used to filters which rows you retrieve from the table.
      * @param key
      * @return DataFrame
      */
    def get(key: String): DataFrame = {
        val db      = hiveConfig.getString(s"tables.${key}.db")
        val table   = hiveConfig.getString(s"tables.${key}.table")
        val columns: Array[String] = if (hiveConfig.hasPath(s"tables.${key}.select")) {
            hiveConfig.getStringList(s"tables.${key}.select").asScala.toArray
        } else {
            Array.empty
        }

        val filter: Option[String] = if (hiveConfig.hasPath(s"tables.${key}.filter")) {
            Some(hiveConfig.getString(s"tables.${key}.filter"))
        } else {
            None
        }

        val df = spark.table(s"${db}.${table}")
        val filteredDf = if (filter.isEmpty) {
            df
        } else {
            df.filter(filter.get)
        }

        val projectedDf = if (columns.isEmpty) {
            filteredDf
        } else {
            filteredDf.selectExpr(columns:_*)
        }

        projectedDf
    }

    /**
      * @todo If defined in the config, check columns / filters
      * @param key
      * @param df
      */
    def save(key: String, df: DataFrame, mode: String = "overwrite"): Unit = {
        val db      = hiveConfig.getString(s"tables.${key}.db")
        val table   = hiveConfig.getString(s"tables.${key}.table")

        df
            .write
            .mode("overwrite")
            .saveAsTable(s"${db}.${table}")
    }
}

object HiveDataManager {
  def apply(@transient spark: org.apache.spark.sql.SparkSession): HiveDataManager = {
    new HiveDataManager(spark, confResource = "hive")
  }

  def apply(@transient spark: org.apache.spark.sql.SparkSession, confResource: String): HiveDataManager = {
    new HiveDataManager(spark, confResource)
  }
}