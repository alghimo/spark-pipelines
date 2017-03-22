package org.alghimo.sparkPipelines

import org.alghimo.sparkPipelines.dataManager.HiveDataManager
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by alghimo on 10/30/2016.
  */
class StageSpec extends FlatSpec with Matchers with WithSharedSparkSession {
  lazy val dataManager: HiveDataManager = new HiveDataManager(spark, Map("hive_config" -> "hive_test"))

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    val table1Data = (1 to 5).map(IdRow(_))
    spark
      .createDataFrame(table1Data)
      .write
      .mode("overwrite")
      .saveAsTable("test.table1")

    val r = scala.util.Random
    val table2Data = (1 to 10).map{value => Table2Row(value, s"${value}_str", value * value)}
    spark
      .createDataFrame(table2Data)
      .write
      .mode("overwrite")
      .saveAsTable("test.table2")
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

    super.afterAll()
  }

  "Stage" should "be able to create a new table" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    table3Stage.run()

    val result = dataManager.get("table3")

    result.count() should be(1)
    result.columns should have size 2
    result.columns should contain allOf("col1", "col3")
  }

  it should "return its inputs" in {
    val table3Stage = new Table3Stage(dataManager, spark)

    table3Stage.inputs should have size 3
    table3Stage.inputs should contain allOf("regular_table", "filtered_table")
  }

  it should "return its outputs" in {
    val table3Stage = new Table3Stage(dataManager, spark)

    val expectedOutputs = Set("table3")
    table3Stage.outputs should have size 1
    table3Stage.outputs.head should be("table3")
  }
}
