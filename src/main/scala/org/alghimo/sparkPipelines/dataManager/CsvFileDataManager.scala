package org.alghimo.sparkPipelines.dataManager

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by D-KR99TU on 21/02/2017.
  */
class CsvFileDataManager(@transient val spark: org.apache.spark.sql.SparkSession, override val confResource: String = "file") extends DataManager with FileConfig {
  import CsvFileDataManager._

  def hasResource(key: String): Boolean = {
    fileConfig.hasPath(s"files.${key}")
  }

  def resourceName(key: String): String = {
    fileConfig.getString(s"files.${key}.path")
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
    val columns: Array[String] = if (fileConfig.hasPath(s"files.${key}.select")) {
      fileConfig.getStringList(s"files.${key}.select").asScala.toArray
    } else {
      Array.empty
    }

    val filter: Option[String] = if (fileConfig.hasPath(s"files.${key}.filter")) {
      Some(fileConfig.getString(s"files.${key}.filter"))
    } else {
      None
    }

    val readOptions = getOptions(key)

    val df = spark
      .read
      .options(readOptions)
      .csv(fileConfig.getString(s"files.${key}.path"))

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
  def save(key: String, df: DataFrame, mode: String): Unit = {
    val target = resourceName(key)
    val options = getOptions(key)
    df
      .write
      .mode(options("mode"))
      .option("header", options("header"))
      .csv(target)
  }

  private def getOptions(key: String) = {
    val tmpOptions: mutable.Map[String, String] = mutable.Map(
      "sep"   -> defaultFieldSeparator,
      "header"      -> defaultHeaders,
      "inferSchema" -> defaultInferSchema,
      "mode"        -> defaultSaveMode
    )

    if (fileConfig.hasPath(s"files.${key}.options")) {
      val optionsConfig = fileConfig.getObject(s"files.${key}.options")
      for (k <- optionsConfig.keySet().asScala) {
        tmpOptions(k) = optionsConfig.get(k).unwrapped().toString
      }
    }

    tmpOptions.toMap
  }
}

object CsvFileDataManager {
  final protected val defaultFieldSeparator = ","
  final protected val defaultHeaders = "true"
  final protected val defaultInferSchema = "true"
  final protected val defaultSaveMode = "overwrite"

  def apply(@transient spark: org.apache.spark.sql.SparkSession): CsvFileDataManager = {
    new CsvFileDataManager(spark, confResource = "file")
  }

  def apply(@transient spark: org.apache.spark.sql.SparkSession, confResource: String ): CsvFileDataManager = {
    new CsvFileDataManager(spark, confResource)
  }
}
