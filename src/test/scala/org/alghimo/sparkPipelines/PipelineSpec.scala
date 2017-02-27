package org.alghimo.sparkPipelines

/**
  * Created by alghimo on 10/30/2016.
  */
class PipelineSpec extends WithSharedSparkSession {
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

    test("Pipeline of two stages - run") {
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

        assert(table3.count() == 1L, "Pipeline - table3 should have one row")
        assert(table3.columns.size == 2L, "Pipeline - table3 should contain two columns")
        assert(table3.columns.contains("col1"), "Pipeline - table3  should contain col1")
        assert(table3.columns.contains("col3"), "Pipeline - table3 should contain col3")
        assert(table4.count() == 2L, "Pipeline - table4 should have two rows")
    }

    test("Pipeline of two stages - inputs") {
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

        val expectedInputs = Set("regular_table", "filtered_table", "table3")
        assert(inputs.size == expectedInputs.size, s"Pipeline - inputs should have ${expectedInputs.size} elements")

        expectedInputs.foreach{input => {
            assert(inputs.contains(input), s"Pipeline inputs should contain '${input}'")
        }}
    }

    test("Pipeline of two stages - outputs") {
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

        val expectedOutputs = Set("table3", "table4")
        assert(outputs.size == expectedOutputs.size, s"Pipeline - outputs should have ${expectedOutputs.size} elements")

        expectedOutputs.foreach{output => {
            assert(outputs.contains(output), s"Pipeline outputs should contain '${output}'")
        }}
    }

    test("Pipeline - nested pipeline") {
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

        assert(table3.count() == 1L, "Nested Pipeline - table3 should have one row")
        assert(table3.columns.size == 2L, "Nested Pipeline - table3 should contain two columns")
        assert(table3.columns.contains("col1"), "Nested Pipeline - table3  should contain col1")
        assert(table3.columns.contains("col3"), "Nested Pipeline - table3 should contain col3")
        assert(table4.count() == 2L, "Nested Pipeline - table4 should have two rows")
    }

    test("Pipeline with nested pipeline - inputs") {
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

        val expectedInputs = Set("regular_table", "filtered_table", "table3")
        assert(inputs.size == expectedInputs.size, s"Nested Pipeline - inputs should have ${expectedInputs.size} elements")

        expectedInputs.foreach{input => {
            assert(inputs.contains(input), s"Nested Pipeline inputs should contain '${input}'")
        }}
    }

    test("Pipeline with nested pipeline - outputs") {
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

        val expectedOutputs = Set("table3", "table4")
        assert(outputs.size == expectedOutputs.size, s"Nested Pipeline - outputs should have ${expectedOutputs.size} elements")

        expectedOutputs.foreach{output => {
            assert(outputs.contains(output), s"Nested Pipeline outputs should contain '${output}'")
        }}
    }
}
