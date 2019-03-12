import State._

import scala.collection.mutable
import org.apache.spark.rdd.RDD
import scala.io.Source

object SequentialDBSCAN {
  var eps = 0.2
  var minPts = 3
  var centroids: mutable.Set[mutable.Set[Record]] = mutable.Set()
  var records: Set[Record] = Set()

  /* Entry point */
  def exec(values: RDD[Record]): Unit = {
    records = values.collect().toSet
    DbScan()
    println("Analize " + records.count(_ => true) + " tweets")
    centroids.foreach(x => {
      println("Cluster size:" + x.size)
      x.foreach(y => println("     " + y.tweet))
    })
  }

  def DbScan(): Unit = {
    records.foreach { x =>
      if (x.state == Unvisited) {
        execute(x)
      }
    }
  }

  def execute(rec: Record): Unit = {
    rec.state = Visited
    rec.neighbors = getNeighbors(rec, records)
    if (rec.neighbors.size < minPts) {
      rec.state = Noise
    } else {
      var cluster = mutable.Set(rec)
      rec.state = ClusterMember
      centroids += cluster
      rec.neighbors.foreach { x =>
        expand(x, rec, cluster)
      }
    }
  }

  def expand(
      newRecord: Record,
      centroid: Record,
      cluster: mutable.Set[Record]
  ): Unit = {
    if (newRecord.state != ClusterMember) {
      newRecord.state = Visited
      newRecord.neighbors = getNeighbors(newRecord, records)
      if (newRecord.neighbors.size >= minPts) {
        //newRecord.neighbors.get.foreach {x => centroid.neighbors.get++ }ù
        centroid.neighbors ++= newRecord.neighbors
      }
    }
    if (newRecord.state != ClusterMember) {
      cluster += newRecord
      newRecord.state = ClusterMember
    }
  }

  def calculateDistance(record1: Record, record2: Record): Double = {
    val temp =
      record1.weighsVector.toArray.union(record2.weighsVector.toSeq).map(_._1)
    val first = temp.map(record1.weighsVector)
    val second = temp.map(record2.weighsVector)
    CosineSimilarity.cosineSimilarity(first, second)
  }

  def getNeighbors(rec: Record, records: Set[Record]): Set[Record] = {
    return records.filter(calculateDistance(_, rec) > eps)
  }
}
