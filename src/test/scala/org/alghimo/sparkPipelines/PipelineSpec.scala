package org.alghimo.sparkPipelines

import org.alghimo.sparkPipelines.dataManager.HiveDataManager
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by alghimo on 10/30/2016.
  */
class PipelineSpec extends FlatSpec with Matchers with WithSharedSparkSession {
  lazy val dataManager: HiveDataManager = new HiveDataManager(spark, "hive_test")

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

    super.beforeAll()
  }

  "Pipeline" should "run with two stages" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    val table4Stage = new Table4Stage(dataManager, spark)
    val pipeline = new Pipeline(
      name = "Test Pipeline",
      stages = Array(table3Stage, table4Stage),
      description = Some("Test two-stage pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )
    pipeline.run

    val table3 = dataManager.get("table3")
    val table4 = dataManager.get("table4")

    table3.count() should be(1)
    table3.columns should have size 2
    table3.columns should contain allOf("col1", "col3")
    table4.count() should be(2)
  }

  it should "get inputs from two stages" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    val table4Stage = new Table4Stage(dataManager, spark)
    val pipeline = new Pipeline(
      name = "Test Pipeline",
      stages = Array(table3Stage, table4Stage),
      description = Some("Test two-stage pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )

    val inputs = pipeline.inputs

    inputs.size should be(3)
    inputs should contain allOf("regular_table", "filtered_table", "table3")
  }

  it should "get the outputs from two stages" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    val table4Stage = new Table4Stage(dataManager, spark)
    val pipeline = new Pipeline(
      name = "Test Pipeline",
      stages = Array(table3Stage, table4Stage),
      description = Some("Test two-stage pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )

    val outputs = pipeline.outputs

    outputs.size should be(2)
    outputs should contain allOf("table3", "table4")
  }

  it should "run a nested pipeline" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    val table4Stage = new Table4Stage(dataManager, spark)
    val innerPipeline = new Pipeline(
      name = "Inner Pipeline",
      stages = Array(table3Stage),
      description = Some("Inner Pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )
    val pipeline = new Pipeline(
      name = "Pipeline with nested Pipeline",
      stages = Array(innerPipeline, table4Stage),
      description = Some("Pipeline with Inner Pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )
    pipeline.run

    val table3 = dataManager.get("table3")
    val table4 = dataManager.get("table4")

    table3.count() should be(1)
    table3.columns should have size 2
    table3.columns should contain allOf("col1", "col3")
    table4.count() should be(2)
  }

  it should "get inputs with nested pipeline" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    val table4Stage = new Table4Stage(dataManager, spark)
    val innerPipeline = new Pipeline(
      name = "Inner Pipeline",
      stages = Array(table3Stage),
      description = Some("Inner Pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )
    val pipeline = new Pipeline(
      name = "Pipeline with nested Pipeline",
      stages = Array(innerPipeline, table4Stage),
      description = Some("Pipeline with Inner Pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )

    val inputs = pipeline.inputs

    inputs.size should be(3)
    inputs should contain allOf("regular_table", "filtered_table", "table3")
  }

  it should "get outputs from nested pipeline" in {
    val table3Stage = new Table3Stage(dataManager, spark)
    val table4Stage = new Table4Stage(dataManager, spark)
    val innerPipeline = new Pipeline(
      name = "Inner Pipeline",
      stages = Array(table3Stage),
      description = Some("Inner Pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )
    val pipeline = new Pipeline(
      name = "Pipeline with nested Pipeline",
      stages = Array(innerPipeline, table4Stage),
      description = Some("Pipeline with Inner Pipeline")
    )(
      dataManager = dataManager,
      spark = spark
    )

    val outputs = pipeline.outputs

    outputs.size should be(2)
    outputs should contain allOf("table3", "table4")
  }
}
