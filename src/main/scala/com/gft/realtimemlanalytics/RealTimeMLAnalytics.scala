/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package com.gft.realtimemlanalytics

import org.apache.spark._

object RealTimeMLAnalytics {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)


      val conf = new SparkConf().setAppName("realTimeMLAnalytics")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val rawData =  sc.textFile(inputFile)

      // counting the labels
      //1,tcp,smtp,SF,950,493,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,1,0.00,0.00,0.00,0.00,0.50,1.00,0.00,138,243,0.95,0.01,0.01,0.01,0.00,0.00,0.00,0.00,normal.
      //0,icmp,ecr_i,SF,1032,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,316,316,0.00,0.00,0.00,0.00,1.00,0.00,0.00,148,3,0.02,0.02,0.02,0.00,0.00,0.00,0.00,0.00,smurf.

      //countByValues returns a map
      //toSeq transforms in ArrayBuffer (which can be sorted)
      rawData.map (_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach (println)
      /*
      (smurf.,280790)
      (neptune.,107201)
      (normal.,97278)
      (back.,2203)
      (satan.,1589)
      (ipsweep.,1247)
      (portsweep.,1040)
      (warezclient.,1020)
      (teardrop.,979)
      (pod.,264)
      (nmap.,231)
      (guess_passwd.,53)
      (buffer_overflow.,30)
      (land.,21)
      (warezmaster.,20)
      (imap.,12)
      (rootkit.,10)
      (loadmodule.,9)
      (ftp_write.,8)
      (multihop.,7)
      (phf.,4)
      (perl.,3)
      (spy.,2)
       */

      // deleting categorical variables
     // val labelsAndData


    }
}
