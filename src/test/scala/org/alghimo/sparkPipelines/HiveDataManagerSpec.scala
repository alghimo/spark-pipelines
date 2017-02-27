package org.alghimo.sparkPipelines

/**
  * Created by alghimo on 10/30/2016.
  */
class HiveDataManagerSpec extends WithSharedSparkSession {
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

    test("Get plain table - no filtering, no projection") {
        val df = dataManager.get("regular_table")

        assert(df.count() == 5L, "Table1 should contain 5 rows")
    }

    test("Get projected table") {
        val df = dataManager.get("projected_table")

        assert(df.columns.size == 2L, "Projected table should contain only two columns")
        assert(df.columns.contains("newCol1"), "Projected table should contain newCol1")
        assert(df.columns.contains("col3"), "Projected table should contain col3")
    }

    test("Get filtered table") {
        val df = dataManager.get("filtered_table")

        assert(df.count == 1L, "Filtered table should contain only one row")
    }

    test("Get filtered and projected table") {
        val df = dataManager.get("filtered_and_projected_table")

        assert(df.count == 2L, "Filtered and projected table should contain only two rows")
        assert(df.columns.size == 2L, "Projected table should contain only two columns")
        assert(df.columns.contains("col2"), "Projected table should contain col2")
        assert(df.columns.contains("col3"), "Projected table should contain col3")
    }

    test("Save a dataframe to a table") {
        val spark = this.spark
        import spark.implicits._
        val newData = Seq(IdRow(7))
        val newDf = spark.createDataFrame(newData)
        dataManager.save("table3", newDf)

        val result = dataManager.get("table3")
        assert(result.count() == 1L, "New table should contain only one row")
        assert(result.as[Int].first == 7, "col1 should be 7 on the new table")
    }
}
