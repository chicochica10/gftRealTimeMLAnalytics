/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package com.gft.realtimemlanalytics

import java.lang.System._

import org.apache.spark._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object RealTimeMLAnalytics {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)


    val conf = new SparkConf().setAppName("realTimeMLAnalytics")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val rawDataAll = sc.textFile(inputFile)

    //work in local with a subset of data
    val rawDataSample = rawDataAll.takeSample(false, 50000)
    val startNanoTime = nanoTime
    val rawData = sc.parallelize(rawDataSample)


    //1,tcp,smtp,SF,950,493,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,1,0.00,0.00,0.00,0.00,0.50,1.00,0.00,138,243,0.95,0.01,0.01,0.01,0.00,0.00,0.00,0.00,normal.
    //0,icmp,ecr_i,SF,1032,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,316,316,0.00,0.00,0.00,0.00,1.00,0.00,0.00,148,3,0.02,0.02,0.02,0.00,0.00,0.00,0.00,0.00,smurf.

    // counting the labels
    //countByValues returns a map
    //toSeq transforms in ArrayBuffer (which can be sorted)

    //rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    /* 23 types:
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

    //1,tcp,smtp,SF,950,493,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,1,0.00,0.00,0.00,0.00,0.50,1.00,0.00,138,243,0.95,0.01,0.01,0.01,0.00,0.00,0.00,0.00,normal.
    //0,icmp,ecr_i,SF,1032,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,316,316,0.00,0.00,0.00,0.00,1.00,0.00,0.00,148,3,0.02,0.02,0.02,0.00,0.00,0.00,0.00,0.00,smurf.

    // deleting categorical variables and creating pairs label,values

    /*val labelsAndData = rawData.map { line =>
      //toBuffer creates a mutable list !!!
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      // Vectors.dense = mlib vectors of double
      // convert buffer os string to array of doubles
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }*/


    //using categorical variables
    def buildCategoricalAndLabel: (String => (String, Vector)) = {

      val splitData = rawData.map(_.split(',')) //RDD [Array[String]]

      //1,tcp,smtp,SF,950,493,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,1,0.00,0.00,0.00,0.00,0.50,1.00,0.00,138,243,0.95,0.01,0.01,0.01,0.00,0.00,0.00,0.00,normal.
      //0,icmp,ecr_i,SF,1032,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,316,316,0.00,0.00,0.00,0.00,1.00,0.00,0.00,148,3,0.02,0.02,0.02,0.00,0.00,0.00,0.00,0.00,smurf.
      //splitData.map (_(1)).foreach(println)
      //tcp
      //tcp
      //icmp
      //splitData.map (_(1)).distinct.foreach(println)
      //tcp
      //icmp
      //splitData.map (_(1)).distinct.collect
      //Array (icmp, tcp)
      //splitData.map (_(1)).distinct.collect.zipWithIndex
      //Array((icmp,0), (tcp,1))

      val protocols = splitData.map(_(1)).distinct.collect.zipWithIndex.toMap // Map(icmp -> 0, tcp -> 1)
      val services = splitData.map(_(2)).distinct.collect.zipWithIndex.toMap
      val tcpStates = splitData.map(_(3)).distinct.collect.zipWithIndex.toMap

      println(s"---> protocols ${protocols.size}")
      protocols.foreach(println)
      println(s"---> services ${services.size}")
      services.foreach(println)
      println(s"---> tcpStates ${tcpStates.size}")
      tcpStates.foreach(println)

      /* tcpStates 9
      (S1,8)
      (RSTR,5)
      (REJ,4)
      (S0,0)
      (RSTOS0,2)
      (SH,6)
      (SF,1)
      (RSTO,7)
      (S2,3)
       */

      (line: String) => {
        val buffer = line.split(',').toBuffer
        val protocol = buffer.remove(1)
        val service = buffer.remove(1)
        val tcpState = buffer.remove(1)
        val label = buffer.remove(buffer.length - 1)
        val bufferData = buffer.map(_.toDouble)

        //mutables...
        val newProtocolFeatures = new Array[Double](protocols.size)
        newProtocolFeatures(protocols(protocol)) = 1.0
        val newServiceFeatures = new Array[Double](services.size)
        newServiceFeatures(services(service)) = 1.0
        val newTcpStateFeatures = new Array[Double](tcpStates.size)
        newTcpStateFeatures(tcpStates(tcpState)) = 1.0

        bufferData.insertAll(1, newTcpStateFeatures)
        bufferData.insertAll(1, newServiceFeatures)
        bufferData.insertAll(1, newProtocolFeatures)
        val vector = Vectors.dense(bufferData.toArray)
        (label, vector)
        //lb
        //81, 4173.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,147.0,105.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,4.0,2.0,0.0,0.0,0.0,0.0,0.5,0.5,0.0,255.0,2.0,0.01,0.84,1.0,0.0,0.0,0.0,0.0,0.0
      }
    }

    val labelsAndData = rawData.map(buildCategoricalAndLabel)

    //(smurf.,[0.0,1032.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,511.0,511.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,255.0,255.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0])
    //(teardrop.,[0.0,28.0,0.0,0.0,3.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,34.0,34.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,255.0,1.0,0.0,0.01,0.0,0.0,0.0,0.0,0.0,0.0])

    //save the values only
    /*
    The call to cache() suggests to Spark that this RDD should be temporarily stored
    after being computed, and furthermore, kept in memory in the cluster. This is helpful
    because the ALS algorithm is iterative, and will typically need to access this data 10
    times or more. Without this, the RDD could be repeatedly recomputed from the orig‐
    inal data each time it is accessed!
     */
    //val data = labelsAndData.values.cache()


    //data.foreach(println)

    // val kmeans = new KMeans()
    // creating a model with K=2 as default
    //org.apache.spark.mllib.clustering.KMeansModel
    //val model = kmeans.run(data)

    //model.clusterCenters is Array[org.apache.spark.mllib.linalg.Vector]
    //model.clusterCenters.foreach(println)
    //[47.979395571029514,1622.078830816566,868.5341828266062,4.453261001578883E-5,0.006432937937735314,1.4169466823205539E-5,0.03451682118132869,1.5181571596291647E-4,0.14824703453301485,0.01021213716043885,1.1133152503947209E-4,3.6435771831099954E-5,0.011351767134933808,0.0010829521072021374,1.0930731549329986E-4,0.0010080563539937655,0.0,0.0,0.0013865835391279706,332.2862475203433,292.9071434354884,0.1766854175944295,0.1766078094004292,0.05743309987449898,0.05771839196793656,0.7915488441762849,0.020981640419416685,0.028996862475203982,232.4707319541719,188.6660459090725,0.7537812031901855,0.030905611108874582,0.6019355289259479,0.0066835148374550625,0.17675395732965873,0.17644162179668482,0.05811762681672762,0.05741111695882669]
    //[2.0,6.9337564E8,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,57.0,3.0,0.79,0.67,0.21,0.33,0.05,0.39,0.0,255.0,3.0,0.01,0.09,0.22,0.0,0.18,0.67,0.05,0.33]

    //counting labels for each cluster
    //scala.collection.Map[(Int, String),Long]
    /*val clusterLabelCount = labelsAndData.map { case (label, data) =>
      val cluster = model.predict(data)
      (cluster, label)
    }.countByValue()*/

    //clusterLabelCount.foreach(println)
    /*
    ((0,portsweep.),1039)
    ((0,rootkit.),10)
    ((0,buffer_overflow.),30)
    ((0,phf.),4)
    ((0,pod.),264)
    ((0,perl.),3)
    ((0,spy.),2)
    ((0,ftp_write.),8)
    ((0,nmap.),231)
    ((0,ipsweep.),1247)
    ((0,imap.),12)
    ((0,warezmaster.),20)
    ((0,satan.),1589)
    ((0,teardrop.),979)
    ((0,smurf.),280790)
    ((0,neptune.),107201)
    ((0,loadmodule.),9)
    ((0,guess_passwd.),53)
    ((0,normal.),97278)
    ((0,land.),21)
    ((0,multihop.),7)
    ((1,portsweep.),1)
    ((0,warezclient.),1020)
    ((0,back.),2203)
     */

    // printing better
    /*clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster,label),count) => println (f"$cluster%1s$label%18s$count%8s")
    }*/

    /*
    0          ipsweep.    1247
    0             land.      21
    0       loadmodule.       9
    0         multihop.       7
    0          neptune.  107201
    0             nmap.     231
    0           normal.   97278
    0             perl.       3
    0              phf.       4
    0              pod.     264
    0        portsweep.    1039
    0          rootkit.      10
    0            satan.    1589
    0            smurf.  280790
    0              spy.       2
    0         teardrop.     979
    0      warezclient.    1020
    0      warezmaster.      20
1        portsweep.       1
     */

    //choosing k
    /* A clustering could be considered good if each data point were near to its closest centroid. So, we define a
    Euclidean distance function, and a function that returns the distance from a data point to its nearest cluster’s
    centroid:
     */

    def distance(a: Vector, b: Vector) = {
      math.sqrt(a.toArray.zip(b.toArray).map(p => p._2 - p._1).map(d => d * d).sum)
    }

    def distanceToCentroid(datum: Vector, model: KMeansModel) = {
      val cluster = model.predict(datum)
      val centroid = model.clusterCenters(cluster)
      distance(centroid, datum)
    }
    //take => Array[Double]
    //data.map(datum => distanceToCentroid(datum, model)).take(10).foreach(println)

    def runKmeans(data: RDD[Vector], k: Int): KMeansModel = {
      val kmeans = new KMeans()

      /**
       * :: Experimental ::
       * Set the number of runs of the algorithm to execute in parallel. We initialize the algorithm
       * this many times with random starting conditions (configured by the initialization mode), then
       * return the best clustering found over any run. Default: 1.
       */
      kmeans.setRuns(10)

      /**
       * Set the distance threshold within which we've consider centers to have converged.
       * If all centers move less than this Euclidean distance, we stop iterating one run.
       * default 1.0e-4
       */
      kmeans.setEpsilon(1.0e-4)
      kmeans.setK(k)
      val model = kmeans.run(data)
      model
    }

    //lets create a function that measures the average distance to centroid, for a model built with a given k:
   /* def clusteringScore(data: RDD[Vector], k: Int) = {
      val model: KMeansModel = runKmeans(data, k)
      val distances = data.map(datum => distanceToCentroid(datum, model)) //RDD [Double]
      distances.mean
    }*/



    //(5 to 40 by 5).map (k => (k,clusteringScore(data, k))).foreach (println)
    /* score decreases as k increases but K-means is not necessarily able to find the optimal clustering for a given k.
     Its iterative process can converge from a random starting point to a local minimum, which may be good but not optimal.
    (5,1749.4955121516966) <- they are means of all the distances from each event to their respective centroid (cluster center)
    (10,1535.43894300135)
    (15,723.1499910483735)
    (20,510.10923307538445)
    (25,400.1899890898291)
    (30,685.0270651245346) <- The random starting set of clusters chosen for k = 30 perhaps led to a particularly
                              suboptimal clustering, or, it may have stopped early before it reached its local
    (35,245.55009752250794)
    (40,241.07419621421363)
     */

    // for 20000 entries
    /*
    (5,1184.1250433712667)
    (10,499.5552463862195)
    (15,370.7597548156588)
    (20,284.4111283036339)
    (25,230.8217305228657)
    (30,155.32910280434814)
    (35,199.43472085055075) <- suboptimal cluster or stopped early
    (40,126.31358250261039)
    time taken: 16.566826616 sec.
     */


    //running with a larger k values from 30 to 100
    //setting the number that k-means runs for one K (see (1) in def clusteringScore)
    //and setting Epsilon that controls de minimum amount of cluster centroid movement that is confederated significant
    //(see (2) in def clusteringScore

    //the range from 30 to 100 is turned into a parallel collection in Scala. This causes
    //the computation for each k to happen in parallel in Spark

    //(30 to 200 by 10).par.map (k => (k,clusteringScore(data, k))).toList.foreach (println)
    /* for 20000 entries from 30 to 100 by 10
    (30,185.26588425078768)
    (40,122.3872294237663)
    (50,89.5272737368863)
    (60,79.33674839693319)
    (70,68.00624198757605)
    (80,51.2739164097593)
    (90,47.293850952405286)
    (100,43.785178713335675)
    time taken: 53.572641739 sec.
     */

    /* for 50000 entries from 30 to 200 by 10*/
    /*
    (30,276.82214276062786)
    (40,154.87436185640516)
    (50,113.03992344338334)
    (60,99.70665650823967)
    (70,84.95954662995699)
    (80,71.84293233894783)
    (90,58.39542697952507)
    (100,53.256214567254695)
    (110,48.649927340061026)
    (120,44.26780005956568)
    (130,42.90919894152105)
    (140,37.74866287621757)
    (150,35.73193789580504)
    (160,38.01959965245554)
    (170,33.9688264397302)
    (180,30.812794478795443)
    (190,30.010233408133246)
    (200,29.10443049867585)
     */

    // val estimatedNanoTime = (nanoTime - startNanoTime) /   1000000000.0
    // println (s"time taken: $estimatedNanoTime sec.")

    // we need to find a point past which increasing k stops reducing the score much...
    // an "elbow" in a grpah of k vs. scores
    // past k = 100 scores reduces slowly
    // let's take a sample for R
    def saveDataWithKmeans(theData: RDD[Vector], k: Int) = {
      val model: KMeansModel = runKmeans(theData, k)
      val sample = theData.map(datum => model.predict(datum) + "," + datum.toArray.mkString(","))
      // not necessary get a sample because is already sampled
      //.sample (false, 0.05) <- get 5% of the data RDD
      sample.saveAsTextFile(outputFile)
    }

    // saveDataWithKmeans(data, 100)

    /*
      the dominant feature of the visualization is its “L” shape. The
      points seem to vary along two distinct dimensions, and little in other dimensions.
      This makes sense, because the data set has two features that are on a much larger scale
      than the others. Whereas most features have values between 0 and 1, the bytes-sent
      and bytes-received features vary from 0 to tens of thousands. The Euclidean distance
      between points is therefore almost completely determined by these two features. It’s
      almost as if the other features don’t exist! So, it’s important to normalize away these
      differences in scale to put features on near-equal footing.
    */

    //Normalization
    /*
      normalized(i) = feature(i) - mean(i) / standard_desviation (i)
     */


    /*def normalize(datum: Vector) = {

      val dataAsArray = data.map(_.toArray)
      val numCols = dataAsArray.first().length
      val n = dataAsArray.count()
      val sums = dataAsArray.reduce((x, y) => x.zip(y).map(p => p._1 + p._2)) //Array [Double] pag. 42 learning spark

      val sumSquares = dataAsArray.fold(new Array[Double](numCols))(//pag. 42 learning spark
        (x, y) => x.zip(y).map(p => p._1 + p._2 * p._2)
      )

      val stdevs = sumSquares.zip(sums).map {
        //Array[Double]
        case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
      }

      val means = sums.map(_ / n) //Array[Double]


      val normalizedArray = (datum.toArray, means, stdevs).zipped.map {
        case (value, mean, stdev) =>
          if (stdev <= 0) value - mean else (value - mean) / stdev
      }
      Vectors.dense(normalizedArray)
    }*/

    def normalize(data: RDD[Vector]): (Vector => Vector) = {

      val dataAsArray = data.map(_.toArray)
      val numCols = dataAsArray.first().length
      val n = dataAsArray.count()
      val sums = dataAsArray.reduce((x, y) => x.zip(y).map(p => p._1 + p._2)) //Array [Double] pag. 42 learning spark

      val sumSquares = dataAsArray.fold(new Array[Double](numCols))(//pag. 42 learning spark
        (x, y) => x.zip(y).map(p => p._1 + p._2 * p._2)
      )

      val stdevs = sumSquares.zip(sums).map {
        //Array[Double]
        case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
      }

      val means = sums.map(_ / n) //Array[Double]

      datum => {
        val normalizedArray = (datum.toArray, means, stdevs).zipped.map {
          case (value, mean, stdev) =>
            if (stdev <= 0) value - mean else (value - mean) / stdev
        }
        Vectors.dense(normalizedArray)
      }
    }



    // choosing k with normalized data

    val normalizedData = labelsAndData.mapValues(normalize(labelsAndData.values)).cache()
    //(30 to 200 by 10).par.map(k => (k, clusteringScore(normalizedData, k))).toList.foreach(println)


    /* for 50000 entries from 30 to 200 by 10
    (30,0.01399495169120432)
    (40,0.009340735885523622)
    (50,0.007958406978349661)
    (60,0.00718693548235823)
    (70,0.006443132924647207)
    (80,0.00587162950866858)
    (90,0.0055866537255241135)
    (100,0.005047951701312448) <- a good choice k=100
    (110,0.004503811904538471)
    (120,0.004488682966870424)
    (130,0.003932718791304399)
    (140,0.0037021398104917845)
    (150,0.0034968242624846546)
    (160,0.0035601552659101396)
    (170,0.0028780955518806126)
    (180,0.002975197553917668)
    (190,0.0028227377594864703)
    (200,0.00285000126640845)
    time taken: 471.371508456 sec.
     */

    /* for 50000 entries from 30 to 200 by 10 with categorical features
    (30,0.257209030174202)
    (40,0.21222868244047066)
    (50,0.1665028248500492)
    (60,0.11939511286991407)
    (70,0.08042702751191264)
    (80,0.05979275529310292)
    (90,0.035696475917705875)
    (100,0.02420220463163872)
    (110,0.02276925610689223)
    (120,0.013597206581047351)
    (130,0.014664051732393105)
    (140,0.010025083655799226)
    (150,0.00946897545564469) <-- the book takes this because 160 (in the book) fails to get a better cluster
    (160,0.009063945656319313)
    (170,0.007582507748014988)
    (180,0.007772404365208013)
    (190,0.006981890784321001)
    (200,0.006696254493897853)

    //the same after refactoring
    (30,0.25703630897563673)
    (40,0.21385255776203335)
    (50,0.17149528845530898)
    (60,0.13581646091407695)
    (70,0.09094145599525512)
    (80,0.06277019016368193)
    (90,0.029908064311320294)
    (100,0.0268375510558802)
    (110,0.023359410305571954)
    (120,0.01757965547167833)
    (130,0.013524272805127583)
    (140,0.011515834594105061)
    (150,0.010654533751611347)
    (160,0.009697304026422255)
    (170,0.008973568906119033)
    (180,0.008062535090005613)
    (190,0.007525899422738656)
    (200,0.006911416534204392)
    time taken: 450.349361484 sec.

     */


    // saveDataWithKmeans (normalizedData, 100)


    // using label with entropy (pag. 95)
    // better for scoring -> weighted average of entropy

    def clusteringMeans(data: RDD[(String, Vector)], k: Int) = {
      val model: KMeansModel = runKmeans(data.values, k)
      val distances = data.mapValues(datum => distanceToCentroid(datum, model)) //RDD [Double]
      distances.values.mean
    }

    def clusteringEntropyScore(data: RDD[(String, Vector)], k: Int) = {
      val model: KMeansModel = runKmeans(data.values, k)
      val labelsAndCluster= data.mapValues(model.predict) //RDD [(String,Int)]
      val clusterAndLabels = labelsAndCluster.map (_.swap) //RDD [(Int,String)]
      val labelsInCluster = clusterAndLabels.groupByKey().values //RDD[(Int,Iterable[String])] --> .values RDD[Iterable[String]]
      labelsInCluster =
    }

    (30 to 200 by 10).par.map(k => (k, clusteringEntropyScore(normalizedData, k))).toList.foreach(println)


    val estimatedNanoTime = (nanoTime - startNanoTime) / 1000000000.0
    println(s"time taken: $estimatedNanoTime sec.")

  }


}


