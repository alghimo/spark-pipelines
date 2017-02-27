package org.alghimo.sparkPipelines

import org.apache.spark.sql.DataFrame

/**
  * Created by D-KR99TU on 21/02/2017.
  */
class CompositeDataManager(dataManagers: List[DataManager], dmStrategy: (List[DataManager], String) => DataManager = CompositeDataManager.headDataManagerStrategy) extends DataManager {
  import CompositeDataManager._

  def hasResource(key: String): Boolean = {
    dataManagers.map(_.hasResource(key)).max
  }

  def resourceName(key: String): String = {
    val dmsWithResource = dmForKey(key)

    dmsWithResource.resourceName(key)
  }

  /**
    * Returns a DataFrame for the provided key.
    * @param key
    * @return DataFrame
    */
  def get(key: String): DataFrame = {
    val dm = dmForKey(key)

    dm.get(key)
  }

  /**
    * Writes the DataFrame to the provided key. Write mode can be one of the standard spark write modes.
    * @param key
    * @param df
    */
  def save(key: String, df: DataFrame, mode: String): Unit = {
    val dm = dmForKey(key)

    dm.save(key, df, mode)
  }

  private def dmForKey(key: String) = {
    val dms = dataManagers.filter(_.hasResource(key))
    dmStrategy(dms, key)
  }
}

object CompositeDataManager {
  def hiveAndCsvDataManager(@transient spark: org.apache.spark.sql.SparkSession, fileConfResource: String = "file", hiveConfResource: String = "hive"): CompositeDataManager = {
    val csvDataManager = CsvFileDataManager(spark, fileConfResource)
    val hiveDataManager = HiveDataManager(spark, hiveConfResource)
    new CompositeDataManager(List(hiveDataManager, csvDataManager), hiveDataManagerStrategy)
  }

  def headDataManagerStrategy(dms: List[DataManager], key: String): DataManager = {
    if (dms.isEmpty) {
      throw new RuntimeException("No data manager has resource" + key)
    }
    dms.head
  }

  def hiveDataManagerStrategy(dms: List[DataManager], key: String): DataManager = {
    val hiveDms = dms.filter(_ match {
      case x: HiveDataManager => true
      case _ => false
    })
    if (hiveDms.isEmpty) {
      throw new RuntimeException("No hive data manager has resource" + key)
    }

    hiveDms.head
  }
}
