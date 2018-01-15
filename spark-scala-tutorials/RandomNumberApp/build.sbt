name := "SimpleRandomNumberApp"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  val log4jVer = "2.8.2"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" withSources(),
    "org.apache.commons" % "commons-csv" % "1.2",
    "org.apache.logging.log4j" % "log4j-api" % log4jVer,
    "org.apache.logging.log4j" % "log4j-core" % log4jVer
  )
}

