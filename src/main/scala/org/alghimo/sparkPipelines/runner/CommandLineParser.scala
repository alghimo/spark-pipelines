package org.alghimo.sparkPipelines.runner

/**
  * Created by D-KR99TU on 20/03/2017.
  */
object CommandLineParser {
  def parse(args: Array[String], defaults: Map[String, String] = Map()): Map[String, String] = {
    if (args.length == 0) {
      return Map[String, String]()
    }

    def doParse(map : Map[String, String], list: List[String]) : Map[String, String] = {
      list match {
        case Nil => map
        case "--debug" :: tail =>
          doParse(map ++ Map("debug" -> "true"), tail)
        case "--app-config" :: value :: tail =>
          doParse(map ++ Map("app_config" -> value), tail)
        case "--file-config" :: value :: tail =>
          doParse(map ++ Map("file_config" -> value), tail)
        case "--hive-config" :: value :: tail =>
          doParse(map ++ Map("hive_config" -> value), tail)
        case "--stages" :: value :: tail if !map.isDefinedAt("from_stage") =>
          doParse(map ++ Map("stages" -> value), tail)
        case "--from-stage" :: value :: tail if !map.isDefinedAt("stages") =>
          doParse(map ++ Map("from_stage" -> value), tail)
        case "--stages" :: value :: tail if map.isDefinedAt("from_stage") =>
          throw new RuntimeException(s"Using both --from-stages and --stages is not allowed")
        case "--from-stage" :: value :: tail if map.isDefinedAt("stages") =>
          throw new RuntimeException(s"Using both --from-stages and --stages is not allowed")
        case string :: opt2 :: tail if opt2(0) == '-' =>
          doParse(map ++ Map("command" -> string), list.tail)
        case string :: Nil =>  doParse(map ++ Map("command" -> string), list.tail)
        case option :: tail => throw new RuntimeException(s"Unknown option '${option}'")
      }
    }
    doParse(defaults, args.toList)
  }
}
