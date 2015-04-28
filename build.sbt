name := "gftRealTimeMLAnalytics"

version := "0.0.1"

scalaVersion := "2.11.6"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
)

//mainClass in Global := Some("com.gft.realtimemlanalytics.RealTimeMLAnalytics")
