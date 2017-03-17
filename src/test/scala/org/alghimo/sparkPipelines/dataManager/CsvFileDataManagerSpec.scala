package org.alghimo.sparkPipelines.dataManager

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.alghimo.sparkPipelines.WithSharedSparkSession
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by D-KR99TU on 14/03/2017.
  */
class CsvFileDataManagerSpec extends FlatSpec with Matchers with WithSharedSparkSession {
  lazy val dataManager: CsvFileDataManager = {
    val originalConfig = ConfigFactory.load("files_test")
    val filePath = getClass.getResource("/files/test_file.csv").getPath
    val file2Path = getClass.getResource("/files/test_separator.csv").getPath
    val newConfig = originalConfig
      .withValue("files.file.no_options.path", ConfigValueFactory.fromAnyRef(filePath))
      .withValue("files.file.options.path", ConfigValueFactory.fromAnyRef(file2Path))
      .withValue("files.file.with_filter.path", ConfigValueFactory.fromAnyRef(filePath))
      .withValue("files.file.with_select.path", ConfigValueFactory.fromAnyRef(filePath))

    new CsvFileDataManager(spark, "files_test") {
      override lazy val fileConfig = newConfig
    }
  }


  "CsvFileDataManager" should "be able to load files with no options in config" in {
    val fileDf = dataManager.get("file.no_options")

    fileDf.columns should contain allOf("col1", "col2", "col3")
    fileDf.count shouldBe 2
  }

  it should "be able to load files with options in config" in {
    val fileDf = dataManager.get("file.options")

    fileDf.columns should contain allOf("col1", "col2", "col3")
    fileDf.count shouldBe 2
  }

  it should "be able to load files with filters in config" in {
    val fileDf = dataManager.get("file.with_filter")

    fileDf.columns should contain allOf("col1", "col2", "col3")
    fileDf.count shouldBe 1
  }

  it should "be able to load files with select in config" in {
    val fileDf = dataManager.get("file.with_select")

    fileDf.columns should contain allOf("new_col1", "col3")
    fileDf.count shouldBe 2
  }
}
