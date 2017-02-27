package org.alghimo.sparkPipelines.runner

import org.alghimo.sparkPipelines.{DataManager, HiveDataManager, Pipeline}
import org.alghimo.sparkPipelines.Utils.withColor
import org.apache.spark.sql.SparkSession

/**
  * Created by D-KR99TU on 21/02/2017.
  */
trait PipelineRunner extends CommandRunner {
  def createSparkSession() = {
    SparkSession
      .builder()
      .appName("RSM Leads")
      .master("yarn")
      .enableHiveSupport()
      .config("spark.dynamicAllocation.enabled", false)
      .config("spark.executor.instances", 16)
      .config("spark.executor.cores", 2)
      .config("spark.executor.memory", "14g")
      .getOrCreate()
  }
  def createDm(@transient spark: SparkSession): DataManager = HiveDataManager(spark)

  def createPipeline(dm: DataManager, @transient spark: SparkSession): Pipeline
  lazy val pipeline = {
    val spark = createSparkSession()
    val dm = createDm(spark)
    createPipeline(dm, spark)
  }

  /**
    * Map where keys are all available actions, and each value is an ModelAction instance, containing:
    * - name: key for the action
    * - description: Human-readable explanation for the action
    * - action: The method to run when the action is called.
    */
  override def commands = Map(
    "help"                -> Command("help", "Show usage", showUsage),
    "input:show"          -> Command("input:show", "Show all the input tables that the job uses", pipeline.showInputs),
    "input:validate"      -> Command("input:validate", "Validate input tables to make sure they are valid", pipeline.validateInput),
    "output:show"         -> Command("output:show", "Show all the output tables and files that the job generates", pipeline.showOutputs),
    "output:pipeline:run" -> Command("output:pipeline:run", "Run the model.", pipeline.run),
    "output:validate"     -> Command("output:validate", "Validate generated tables", pipeline.validateOutput)
  )

  def params: Map[String, String] = Map()

  def main(args: Array[String]): Unit = {
    if (args.size < 1) {
      showUsage()
    } else {
      run(args(0))
    }
  }
}
