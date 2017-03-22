package org.alghimo.sparkPipelines.dataManager

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.alghimo.sparkPipelines.{IdRow, Table2Row, WithSharedSparkSession}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by D-KR99TU on 17/03/2017.
  */
class CompositeDataManagerSpec  extends FlatSpec with Matchers with WithSharedSparkSession {
  def createDataManager(strategy: (Seq[DataManager], String) => DataManager) = {
    val hiveDataManager: HiveDataManager = new HiveDataManager(spark, Map("hive_config" -> "composite_hive_test"))
    val fileDataManager: CsvFileDataManager = {
      val originalConfig = ConfigFactory.load("composite_files_test")
      val filePath = getClass.getResource("/files/test_file.csv").getPath
      val file2Path = getClass.getResource("/files/test_separator.csv").getPath
      val newConfig = originalConfig
        .withValue("files.key1.path", ConfigValueFactory.fromAnyRef(filePath))
        .withValue("files.key3.path", ConfigValueFactory.fromAnyRef(file2Path))

      new CsvFileDataManager(spark, Map("file_config" -> "composite_files_test")) {
        override lazy val fileConfig = newConfig
      }
    }
    new CompositeDataManager(Seq(fileDataManager, hiveDataManager), Map(), strategy)
  }

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

  "CompositeDataManager" should "get the DataFrame when just one DataManager has it - file manager" in {
    val dataManager: CompositeDataManager = createDataManager(CompositeDataManager.hiveDataManagerStrategy)
    val df = dataManager.get("key1")

    df.columns should contain allOf("col1", "col2", "col3")
    df.count shouldBe 2
  }

  it should "get the DataFrame when just one DataManager has it - hive manager" in {
    val dataManager: CompositeDataManager = createDataManager(CompositeDataManager.hiveDataManagerStrategy)
    val df = dataManager.get("key2")

    df.columns should contain allOf("new_col1", "col3")
    df.count shouldBe 5
  }

  it should "call the right dm according to the strategy - hiveDataManagerStrategy" in {
    val dataManager: CompositeDataManager = createDataManager(CompositeDataManager.hiveDataManagerStrategy)
    val df = dataManager.get("key3")

    df.columns should contain allOf("col1", "col2", "col3")
    df.count shouldBe 5
  }

  it should "call the right dm according to the strategy - headDataManagerStrategy" in {
    val dataManager: CompositeDataManager = createDataManager(CompositeDataManager.headDataManagerStrategy)
    val df = dataManager.get("key3")

    df.columns should contain allOf("col1", "col2", "col3")
    df.count shouldBe 2
  }
}

