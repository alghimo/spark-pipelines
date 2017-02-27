package org.alghimo.sparkPipelines.runner

/**
  * Created by D-KR99TU on 21/02/2017.
  */
case class Command(command: String, description: String, action: () => Any)
