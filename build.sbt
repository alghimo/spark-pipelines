name := "spark-pipelines"
organization := "org.alghimo"
val sparkVersion = "2.1.0"
version := s"spark_${sparkVersion}_0.2.5-SNAPSHOT"
publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

scalaVersion := "2.11.8"
parallelExecution in test := false
sourcesInBase := false

val scalaTestVersion        = "3.0.1"
val sparkTestingBaseVersion = "0.6.0"
val typesafeConfigVersion   = "1.3.0"
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.typesafe" % "config" % typesafeConfigVersion
libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % (s"${sparkVersion}_${sparkTestingBaseVersion}") % "test"
