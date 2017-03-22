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
        case "--file-config" :: value :: tail =>
          doParse(map ++ Map("file_config" -> value), tail)
        case "--hive-config" :: value :: tail =>
          doParse(map ++ Map("hive_config" -> value), tail)
        case "--db" :: value :: tail =>
          doParse(map ++ Map("db" -> value), tail)
        case "--table-prefix" :: value :: tail =>
          doParse(map ++ Map("table_prefix" -> value), tail)
        case string :: opt2 :: tail if opt2(0) == '-' =>
          doParse(map ++ Map("command" -> string), list.tail)
        case string :: Nil =>  doParse(map ++ Map("command" -> string), list.tail)
        case option :: tail => throw new RuntimeException(s"Unknown option '${option}'")
      }
    }
    doParse(defaults, args.toList)
  }
}
