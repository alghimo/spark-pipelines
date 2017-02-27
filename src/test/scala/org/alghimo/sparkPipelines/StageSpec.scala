package org.alghimo.sparkPipelines

/**
  * Created by alghimo on 10/30/2016.
  */
class StageSpec extends WithSharedSparkSession {
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

    test("Stage creating new table") {
        val table3Stage = new Table3Stage(dataManager, spark)
        table3Stage.run()

        val result = dataManager.get("table3")

        assert(result.count() == 1L, "Result of Table3Stage should contain one row")
        assert(result.columns.size == 2L, "Result of Table3Stage should contain two columns")
        assert(result.columns.contains("col1"), "Result of Table3Stage should contain col1")
        assert(result.columns.contains("col3"), "Result of Table3Stage should contain col3")
    }

    test("Test stage inputs") {
        val table3Stage = new Table3Stage(dataManager, spark)

        val expectedInputs = Set("regular_table", "filtered_table")
        assert(expectedInputs.size == table3Stage.inputs.size)
        for (input <- expectedInputs) {
            assert(table3Stage.inputs.contains(input), s"Table3Stage should contain input '${input}'")
        }
    }

    test("Test stage outputs") {
        val table3Stage = new Table3Stage(dataManager, spark)

        val expectedOutputs = Set("table3")
        assert(expectedOutputs.size == table3Stage.outputs.size)
        for (output <- expectedOutputs) {
            assert(table3Stage.outputs.contains(output), s"Table3Stage should contain output '${output}'")
        }
    }
}
