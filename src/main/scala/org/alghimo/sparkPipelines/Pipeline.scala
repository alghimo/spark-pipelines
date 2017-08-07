package org.alghimo.sparkPipelines

import com.typesafe.config.Config
import org.alghimo.sparkPipelines.Utils.withColor
import org.alghimo.sparkPipelines.dataManager.DataManager
import org.apache.spark.sql.SparkSession

/**
  * A pipeline is just a set of stages that are executed sequentially.
  * Note that a pipeline is also a Stage on its own, meaning that you can nest pipelines inside another pipeline.
  * @param name
  * @param stages
  * @param description
  * @param dataManager
  * @param spark
  */
class Pipeline(override val name: String, val stages: Array[Stage], override val description: Option[String] = None)
              (override val dataManager: DataManager, @transient override val spark: SparkSession, config: Config, options: Map[String, String])
  extends Stage(name, description)(dataManager, spark, config) {

  override def inputs: Set[String] = {
    stages
      .map(_.inputs)
      .reduce(_ ++ _)
  }

  override def outputs: Set[String] = {
    stages
      .map(_.outputs)
      .reduce(_ ++ _)
  }

  /**
    * @todo: Use logging
    */
  def run(): Unit = {
    if (options.isDefinedAt("stages")) {
      val stageIdxs = options("stages").split(",").map(_.trim.toInt)
      doRunStages(stageIdxs)
    } else if (options.isDefinedAt("from_stage")) {
      val stageIdx = options("from_stage").trim.toInt
      doRunFromStage(stageIdx)
    } else {
      doRun()
    }
  }

  override def validateInput(): Boolean = {
    stages.map(_.validateInput()).reduce(_ & _)
  }

  override def validateOutput(): Boolean = {
    stages.map(_.validateOutput()).reduce(_ & _)
  }

  override def showInputs(prevIndent: String , indentation: String): Unit = {
    println(s"$prevIndent[Pipeline ${withColor(name)} inputs]:")
    stages.foreach(stage => {
      stage.showInputs(prevIndent + indentation, indentation)
    })
  }

  override def showOutputs(prevIndent: String, indentation: String): Unit = {
    println(s"$prevIndent[Pipeline ${withColor(name)} outputs]:")
    stages.foreach(stage => {
      stage.showOutputs(prevIndent + indentation, indentation)
    })
  }

  def showStages(): Unit = {
    println(withColor(s"Stages in pipeline $name"))
    stages
      .zipWithIndex
      .foreach {
        case(s, idx) => println(s"${withColor(idx.toString)} : ${s.name}")
      }
  }

  def runStages(): Unit = {
    showStages()
    val rawStageNumbers = scala.io.StdIn.readLine(withColor("Comma-separated list of stages to run: "))
    val stageIdxs = rawStageNumbers.split(",").map(_.trim.toInt)

    doRunStages(stageIdxs)
  }

  def runFromStage(): Unit = {
    showStages()
    print(withColor("\nChoose stage number to start from: "))
    val stageIdx = scala.io.StdIn.readInt()

    doRunFromStage(stageIdx)
  }

  protected def doRun() = {
    stages.foreach{stage => {
      println(s"Running stage '${withColor(stage.name)}'")
      stage.run()
    }}
  }

  protected def doRunFromStage(stageIdx: Int) = {
    stages
      .zipWithIndex
      .filter { case (s, idx) => idx >= stageIdx }
      .map(_._1)
      .foreach{stage => {
        println(s"Running stage '${withColor(stage.name)}'")
        stage.run()
      }}
  }

  protected def doRunStages(stageIdxs: Seq[Int]) = {
    stages
      .zipWithIndex
      .filter { case (s, idx) => stageIdxs.contains(idx) }
      .map(_._1)
      .foreach{stage => {
        println(s"Running stage '${withColor(stage.name)}'")
        stage.run()
      }}
  }
}
