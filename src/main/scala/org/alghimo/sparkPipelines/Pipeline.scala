package org.alghimo.sparkPipelines

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
              (override val dataManager: DataManager, @transient override val spark: SparkSession)
    extends Stage(name, description)(dataManager, spark) {

    override def inputs = {
        stages
            .map(_.inputs)
            .reduce(_ ++ _)
    }

    override def outputs = {
        stages
            .map(_.outputs)
            .reduce(_ ++ _)
    }

    /**
      * @todo: Use logging
      */
    def run(): Unit = {
        stages.foreach{stage => {
            println(s"Running stage '${stage.name}'")
            stage.run
        }}
    }

    override def validateInput() = {
        stages.map(_.validateInput).reduce(_ & _)
    }

    override def validateOutput() = {
        stages.map(_.validateOutput).reduce(_ & _)
    }

  override def showInputs(prevIndent: String , indentation: String) = {
    println(s"${prevIndent}[Pipeline ${withColor(name)} inputs]:")
    stages.foreach(stage => {
      stage.showInputs(prevIndent + indentation, indentation)
    })
  }

  override def showOutputs(prevIndent: String, indentation: String) = {
    println(s"${prevIndent}[Pipeline ${withColor(name)} outputs]:")
    stages.foreach(stage => {
      stage.showOutputs(prevIndent + indentation, indentation)
    })
  }
}
