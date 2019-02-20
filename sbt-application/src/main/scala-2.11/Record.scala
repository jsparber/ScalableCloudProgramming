object State extends  Enumeration{
  type State = Value
  val Visited, Unvisited, Noise, ClusterMember = Value
}

import State._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._

import scala.collection.mutable
@SerialVersionUID(100L)
class Record (val doc: String, var vector: Map[String, Double]) extends Serializable {
  val tweet: String = doc
  var weighsVector = vector.withDefaultValue(0.0)
  var state: State = Unvisited
  var neighbors: Set[Record] = Set()

  def apply_idf (idf: Map[String, Double]): Unit = {
    this.weighsVector.transform((key, n) => n * idf(key))
  }

  override def toString: String =  this.tweet + "\n" + this.weighsVector
}


