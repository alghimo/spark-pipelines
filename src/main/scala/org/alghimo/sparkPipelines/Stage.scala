package org.alghimo.sparkPipelines

import org.alghimo.sparkPipelines.Utils.withColor
import org.alghimo.sparkPipelines.dataManager.DataManager
import org.apache.spark.sql.SparkSession

/**
  * Base class for stages.
  * The method that MUST be implemented is the "run" method.
  * You can still override the methods from other traits, such as inputs / outputs atributes and validateInput / validateOutput.
  * @param name
  * @param description
  * @param dataManager
  * @param spark
  */
abstract class Stage
(val name: String, val description: Option[String] = None)
(val dataManager: DataManager, @transient val spark: SparkSession)
  extends Runnable
    with WithManagedData
    with InputData
    with OutputData {

  protected val defaultIndentation = "\t"

  def showInputs(prevIndent: String, indentation: String): Unit = {
    println(s"${prevIndent}[Stage ${withColor(name)} inputs]:")
    inputs.foreach(input => {
      val indent = prevIndent + indentation
      val table = dataManager.resourceName(input)
      println(withColor(indent + table))
    })
  }

  def showInputs(): Unit = {
    showInputs(prevIndent = "", indentation = defaultIndentation)
  }

  def showOutputs(prevIndent: String, indentation: String): Unit = {
    println(s"${prevIndent}[Stage ${withColor(name)} outputs]:")
    outputs.foreach(output => {
      val indent = prevIndent + indentation
      val table = dataManager.resourceName(output)
      println(withColor(indent + table))
    })
  }

  def showOutputs(): Unit = {
    showOutputs(prevIndent = "", indentation = defaultIndentation)
  }
}
