object State extends Enumeration {
  type State = Value
  val Visited, Unvisited, Noise, ClusterMember = Value
}

import State._

import scala.collection.immutable
@SerialVersionUID(100L)
class Record(val doc: TweetTF, idf: Map[String, Double]) extends Serializable {
  val tweet: String = doc.doc
  val weighsVector = doc.apply_idf(idf)
  var state: State = Unvisited
  var neighbors: Set[Record] = Set()

  override def toString: String = this.tweet + "\n" + this.weighsVector
}

@SerialVersionUID(100L)
class TweetTF(val doc: String, var vector: Map[String, Double])
    extends Serializable {
  val tweet: String = doc
  val weighsVector = vector.withDefaultValue(0.0)

  def apply_idf(idf: Map[String, Double]): Map[String, Double] = {
    this.weighsVector.transform((key, n) => n * idf(key)).withDefaultValue(0.0)
  }

  override def toString: String = this.tweet + "\n" + this.weighsVector
}
