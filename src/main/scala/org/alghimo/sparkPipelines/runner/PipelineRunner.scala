package org.alghimo.sparkPipelines.runner

import com.typesafe.config.{Config, ConfigFactory}
import org.alghimo.sparkPipelines.Pipeline
import org.alghimo.sparkPipelines.dataManager.{DataManager, HiveDataManager}
import org.apache.spark.sql.SparkSession

/**
  * Created by D-KR99TU on 21/02/2017.
  */
trait PipelineRunner extends CommandRunner {
  def appName: String
  def createSparkSession(options: Map[String, String]) = {
    SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }
  def createDm(@transient spark: SparkSession, options: Map[String, String]): DataManager = HiveDataManager(spark, options)

  def createPipeline(dm: DataManager, @transient spark: SparkSession, options: Map[String, String], config: Config): Pipeline
  var pipeline: Pipeline = _

  /**
    * Map where keys are all available actions, and each value is an ModelAction instance, containing:
    * - name: key for the action
    * - description: Human-readable explanation for the action
    * - action: The method to run when the action is called.
    */
  override def commands = Map(
    "help"               -> Command("help", "Show usage", showUsage),
    "input:show"         -> Command("input:show", "Show all the input tables that the job uses", pipeline.showInputs),
    "input:validate"     -> Command("input:validate", "Validate input tables to make sure they are valid", pipeline.validateInput),
    "output:show"        -> Command("output:show", "Show all the output tables and files that the job generates", pipeline.showOutputs),
    "output:validate"    -> Command("output:validate", "Validate generated tables", pipeline.validateOutput),
    "pipeline:run"       -> Command("pipeline:run", "Run the pipeline.", pipeline.run),
    "stages:show"        -> Command("stages:show", "Show pipeline stages.", pipeline.showStages),
    "pipeline:runCustom" -> Command("pipeline:runCustom", "Runs a custom number of stages.", pipeline.runStages),
    "pipeline:runFrom"   -> Command("pipeline:runFrom", "Runs the pipeline starting from a specific stage.", pipeline.runFromStage)
  )

  def params: Map[String, String] = Map()

  def createConfig(spark: SparkSession, options: Map[String, String]) = {
    val configFile = options("app_config")
    val strConf    = spark.read.textFile(configFile).collect().mkString("\n")
    ConfigFactory.parseString(strConf).resolve()
  }

  def main(args: Array[String]): Unit = {
    val options    = CommandLineParser.parse(args)
    println("Parsed options: ")
    for ((k, v) <- options) {
      println(s"$k -> $v")
    }

    val spark  = createSparkSession(options)
    val config = createConfig(spark, options)

    pipeline = {
      val dm = createDm(spark, options)
      createPipeline(dm, spark, options, config)
    }

    if (args.size < 1) {
      showUsage()
    } else {
      run(args(0))
    }

    System.exit(0)
  }
}
