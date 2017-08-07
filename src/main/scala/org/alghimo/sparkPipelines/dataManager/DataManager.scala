package org.alghimo.sparkPipelines.dataManager

import org.apache.spark.sql.DataFrame

/**
  * Generic interface for DataManager implementations
  */
trait DataManager {
  def options: Map[String, String]

  def hasResource(key: String): Boolean

  def resourceName(key: String): String

  def resourceExists(key: String): Boolean = true

  /**
    * Returns a DataFrame for the provided key.
    * @param key
    * @return DataFrame
    */
  def get(key: String): DataFrame

  def getIfExists(key: String): Option[DataFrame] = if (resourceExists(key)) {
    Some(get(key))
  } else {
    None
  }

  /**
    * Writes the DataFrame to the provided key. Write mode can be one of the standard spark write modes.
    * @param key
    * @param df
    * @param mode
    */
  def save(key: String, df: DataFrame, mode: String = "overwrite"): Unit
}
