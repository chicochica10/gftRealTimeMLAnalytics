name := "gftRealTimeMLAnalytics"

version := "0.0.1"
  
scalaVersion := "2.10.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.0"
)

//mainClass in Global := Some("com.gft.realtimemlanalytics.RealTimeMLAnalytics")
