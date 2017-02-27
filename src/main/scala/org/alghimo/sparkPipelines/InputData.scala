package org.alghimo.sparkPipelines

/**
  * Represents an element that uses input data.
  */
trait InputData {
  /**
    * List of inputs. Each input must correspond with a key in your data manager.
    */
  def inputs: Set[String] = Set.empty

  /**
    * Override this method in your stage / pipeline if you want to perform input validation.
    */
  def validateInput(): Boolean = { true }
}
