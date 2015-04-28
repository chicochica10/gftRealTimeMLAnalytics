package com.gft.realtimemlanalytics

import org.apache.spark.SparkContext

/**
 * Created by chicochica10 on 28/04/15.
 */
object RealTimeMLAnalytics {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: [sparkmaster] [inputfile]")
      sys.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)
    val result = input.map(_.toInt).aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = result._1 / result._2.toFloat
    println(result)
  }
}
