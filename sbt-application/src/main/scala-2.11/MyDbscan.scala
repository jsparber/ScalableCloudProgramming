import State._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._

import scala.collection.mutable


object MyDbscan {
  var records: Set[Record] = Set()
  val dictionary: Array[String] = Array()
  var eps = 0.2
  var minPts = 3
  var centroids :scala.collection.mutable.Set[scala.collection.mutable.Set[Record]] = scala.collection.mutable.Set()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DBSCAN")
    val sc = new SparkContext(conf)
    val input = sc.textFile("tweets.txt")
    val NDocuments = input.count()
    val words = input.flatMap(_.split(" ")).map(
      _.filterNot(x => x == ',' || x == '!' || x == '?' || x== '''))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    words.foreach{case (key, value)=> dictionary +: key}

    var occurrences: scala.collection.mutable.Map[String, Int]  = scala.collection.mutable.Map()
    words.foreach{case (key, value)=> occurrences += (key -> 0)}

    words.foreach {case (key, value) => println (key + "-->" + value)}
/*
    input.foreach(x=> createRecord(x, dictionary.size, sc))
    dictionary.foreach(word => records.foreach(rec => if (isContained(sc.parallelize(rec.tweetWords), word) )
    increase(word, occurrences)))

*/

    //  records.foreach{x => x.tweet.split()}

    records.foreach{
      x => if (x.isVisited == Unvisited) {execute(x)}
    }

  }

 /* def increase (word: String, occ: scala.collection.mutable.Map[String, Int]): Unit ={
    occ.foreach{case (key, value) => if (key equals word ) value += 1}
  }

  def isContained (words: RDD[(String, Int)], myword: String): Boolean ={
    var res = false
    words.foreach{case (key, value)=> if (key equals myword) res = true}
    return res
  }
*/
  def createRecord (tweet: String, dictionarySize: Int, cont: SparkContext): Unit ={
   /* var newRecord = new Record(tweet, dictionarySize)
    records += newRecord
    var tweetWds  = tweet.split(" ").map(
      _.filterNot(x => x == ',' || x == '!' || x == '?' || x== '''))
    newRecord.tweetWords = cont.parallelize(tweetWds).map(word => (word, 1))
      .reduceByKey(_ + _).collect()  //don't know why it doesn't work
      */
  }

  def execute (rec: Record): Unit ={
    rec.isVisited = Visited
    rec.neighbors = getNeighbors(rec)
    if (rec.neighbors.size < minPts){
      rec.isVisited = Noise
    } else {
      var centroid = scala.collection.mutable.Set(rec)
      rec.isVisited = ClusterMember
      centroids += centroid
      expandCluster(rec, centroid)
    }
  }

  def expandCluster (rec: Record, cluster: scala.collection.mutable.Set[Record]): Unit ={
    rec.neighbors.foreach{x => expand(x, rec, cluster)}
  }

  def expand (newRecord: Record, centroid: Record, cluster: scala.collection.mutable.Set[Record]): Unit ={
    if (newRecord.isVisited == Unvisited){
      newRecord.isVisited = Visited
      newRecord.neighbors = getNeighbors(newRecord)
      if (newRecord.neighbors.size >= minPts){
        newRecord.neighbors.foreach {x => centroid.neighbors += x}
      }
    }
    if (newRecord.isVisited != ClusterMember){
      cluster += newRecord
      newRecord.isVisited = ClusterMember
    }
  }

  def calculateDistance (record1 : Record, record2: Record): Double = {

    return 0.5}

  def getNeighbors (rec: Record): Set[Record] ={
    return records.filter(calculateDistance(_, rec) < eps)
  }

}
