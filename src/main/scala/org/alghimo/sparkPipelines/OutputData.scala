package org.alghimo.sparkPipelines

/**
  * Represents an element that creates output data.
  */
trait OutputData {
    /**
      * List of outputs. Each input must correspond with a key in your data manager.
      */
    def outputs: Set[String] = Set.empty

    /**
      * Override this method in your stage / pipeline if you want to perform output validation.
      */
    def validateOutput(): Boolean = { true }
}
