package org.alghimo.sparkPipelines.dataManager

import org.alghimo.sparkPipelines.{IdRow, Table2Row, WithSharedSparkSession}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by alghimo on 10/30/2016.
  */
class HiveDataManagerSpec extends FlatSpec with Matchers with WithSharedSparkSession {
  lazy val dataManager: HiveDataManager = new HiveDataManager(spark, "hive_test")

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    val table1Data = (1 to 5).map(IdRow(_))
    val idRowsDf = spark.createDataFrame(table1Data)
    idRowsDf
      .write
      .mode("overwrite")
      .saveAsTable("test.table1")
    val table2Data = (1 to 5).map{value => Table2Row(value, s"${value}_str", value * value)}
    spark
      .createDataFrame(table2Data)
      .write
      .mode("overwrite")
      .saveAsTable("test.table2")

    idRowsDf
      .write
      .mode("overwrite")
      .saveAsTable("test.table3")
  }

  override def afterAll(): Unit = {
    val spark = this.spark
    import spark.implicits._

    spark.catalog.listTables("test")
      .select('name)
      .as[String]
      .collect()
      .foreach(t => spark.sql(s"DROP TABLE test.${t}"))

    spark.sql("DROP DATABASE test")

    super.beforeAll()
  }

  "HiveDataManager" should "get a plain table with no filtering and no projection" in {
    val df = dataManager.get("regular_table")

    df.count() should be(5)
  }

  it should "get a projected table" in {
    val df = dataManager.get("projected_table")

    df.columns should have size 2
    df.columns should contain allOf("newCol1", "col3")
  }

  it should "get a filtered table" in {
    val df = dataManager.get("filtered_table")

    df.count should be(1)
  }

  it should "get a filtered and projected table" in {
    val df = dataManager.get("filtered_and_projected_table")

    df.count should be(2)
    df.columns should have size 2
    df.columns should contain allOf("col2", "col3")
  }

  it should "save a dataframe to a table" in {
    val spark = this.spark
    import spark.implicits._
    val newData = Seq(IdRow(7))
    val newDf = spark.createDataFrame(newData)
    dataManager.save("table3", newDf)

    val result = dataManager.get("table3")
    result.count should be(1)
    result.as[Int].first should be(7)
  }
}
