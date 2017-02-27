package org.alghimo.sparkPipelines.runner

import org.alghimo.sparkPipelines.Utils.withColor

/**
  * Created by D-KR99TU on 21/02/2017.
  */
trait CommandRunner {
  /**
    * Map where keys are all available actions, and each value is a Command instance, containing:
    * - name: key for the action
    * - description: Human-readable explanation for the action
    * - action: The method to run when the action is called.
    */
  def commands: Map[String, Command]

  def run(command: String) = {
    commands.get(command) match {
      case None => throw new NoSuchElementException(s"Command${command} doesn't exist")
      case Some(Command(_, _, action)) => action()
    }
  }

  /**
    * Display the model usage instructions.
    *
    * @todo: Add the params to the output.
    */
  def showUsage(): Unit = {
    val className = this.getClass.getCanonicalName
    println(
      s"""Usage:
            spark-submit <OPTS> --class ${className} <APP_JAR> """ + withColor("<action> --param-1 val1 --param-n valn"))
    println(s"Available actions:")
    for (command <- commands.keySet.toArray.sorted) {
      println(withColor(command) + ": " + commands(command).description)
    }
  }
}
