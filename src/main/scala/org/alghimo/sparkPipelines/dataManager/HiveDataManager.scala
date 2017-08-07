package org.alghimo.sparkPipelines.dataManager

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * Concrete implementation of a DataManager that handles reading and writing to Hive.
  * @param spark
  * @param options Map of Options, available options are:
  * "db" -> Will replace the 'db' value of any table included in "pipeline.replaceable_tables" config.
  * "table_prefix" -> Will prepend the 'table_prefix' to the name of any table included in "pipeline.replaceable_tables" config.
  */
class HiveDataManager(@transient val spark: SparkSession, override val options: Map[String,String] = Map()) extends DataManager with HiveConfig {
  def hasResource(key: String): Boolean = {
    hiveConfig.hasPath(s"tables.${key}")
  }

  override def resourceExists(key: String): Boolean = {
    val db      = hiveConfig.getString(s"tables.${key}.db")
    val table   = hiveConfig.getString(s"tables.${key}.table")
    spark.catalog.tableExists(db, table)
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
      .mode(mode)
      .saveAsTable(s"${db}.${table}")
  }
}

object HiveDataManager {
  def apply(@transient spark: org.apache.spark.sql.SparkSession): HiveDataManager = {
    new HiveDataManager(spark, Map())
  }

  def apply(@transient spark: org.apache.spark.sql.SparkSession, options: Map[String, String]): HiveDataManager = {
    new HiveDataManager(spark, options)
  }
}