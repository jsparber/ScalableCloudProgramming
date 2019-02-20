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
   /* val input = sc.textFile("tweets.txt")
   val words = input.flatMap(_.split(" ")).map(
      _.filterNot(x => x == ',' || x == '!' || x == '?' || x== '''))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    words.foreach{case (key, value)=> dictionary +: key}

    var occurrences: scala.collection.mutable.Map[String, Int]  = scala.collection.mutable.Map()
    words.foreach{case (key, value)=> occurrences += (key -> 0)}

    words.foreach {case (key, value) => println (key + "-->" + value)}

    input.foreach(x=> createRecord(x, dictionary.size, sc))
    dictionary.foreach(word => records.foreach(rec => if (isContained(sc.parallelize(rec.tweetWords), word) )
    increase(word, occurrences)))

*/

    //  records.foreach{x => x.tweet.split()}

    records.foreach{
      x => if (x.state == Unvisited) {execute(x)}
    }

  }

  /*def createRecord (tweet: String, dictionarySize: Int, cont: SparkContext): Unit ={
    var newRecord = new Record(tweet, dictionarySize)
    records += newRecord
    var tweetWds  = tweet.split(" ").map(
      _.filterNot(x => x == ',' || x == '!' || x == '?' || x== '''))
    newRecord.tweetWords = cont.parallelize(tweetWds).map(word => (word, 1))
      .reduceByKey(_ + _).collect()  //don't know why it doesn't work

  }*/

  def execute (rec: Record): Unit ={
    rec.state = Visited
    rec.neighbors = getNeighbors(rec)
    if (rec.neighbors.size < minPts){
      rec.state = Noise
    } else {
      var centroid = scala.collection.mutable.Set(rec)
      rec.state= ClusterMember
      centroids += centroid
      expandCluster(rec, centroid)
    }
  }

  def expandCluster (rec: Record, cluster: scala.collection.mutable.Set[Record]): Unit ={
    rec.neighbors.foreach{x => expand(x, rec, cluster)}
  }

  def expand (newRecord: Record, centroid: Record, cluster: scala.collection.mutable.Set[Record]): Unit ={
    if (newRecord.state == Unvisited){
      newRecord.state = Visited
      newRecord.neighbors = getNeighbors(newRecord)
      if (newRecord.neighbors.size >= minPts){
        newRecord.neighbors.foreach {x => centroid.neighbors += x}
      }
    }
    if (newRecord.state != ClusterMember){
      cluster += newRecord
      newRecord.state = ClusterMember
    }
  }

  def calculateDistance (record1 : Record, record2: Record): Double = {
    val temp = record1.weighsVector.toArray.union(record2.weighsVector.toSeq).map(_._1)
    val first = temp.map(record1.weighsVector)
    val second = temp.map(record2.weighsVector)

    return CosineSimilarity.cosineSimilarity(first,second)
  }

  def getNeighbors (rec: Record): Set[Record] ={
    return records.filter(calculateDistance(_, rec) < eps)
  }
}
