import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.graphx.{VertexRDD, EdgeDirection, Edge, Graph}
import scala.util.Random
import State._

object ngDBSCAN {
  val eps = 0.05
  val minPts = 3
  val k = 2 //should be 5
  // p limits the number of comparisons in extreme cases during Phase 1
  val p = 3
  val Mmax = 2 * k
  val TN = 0.7
  val TR = 0.01

  def exec(sc: SparkContext, records: RDD[Record]) {
    //PHASE 1
    /* We could use also zipWithIndex() */
    val docsCount = records.count
    val nodes = records.zipWithUniqueId().map(_.swap);
    val emptyEdges
        : org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Double]] =
      sc.parallelize(Seq())
    var epsGraph = Graph(nodes, emptyEdges)
    val collection = nodes.collect
    /* Create radom edges with weight based on the distance between the nodes */
    val randomEdges
        : org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Double]] =
      nodes.flatMap(
        n1 =>
          Random
            .shuffle(collection.toList)
            .take(k)
            .map(n2 => Edge(n1._1, n2._1, calculateDistance(n1._2, n2._2)))
      )
    println("Random edges" + randomEdges.count)
    var nGraph = Graph(nodes, randomEdges)
    var terminate = false;
    while (!terminate) {
      // Add reverse edges
      val rEdges = nGraph.edges.reverse
      nGraph = Graph(nGraph.vertices, (nGraph.edges ++ rEdges).distinct())
      // add more edges to nGraph
      // FIXME: the number of neighbors should be limited by pk
      val neighbors = nGraph.collectNeighbors(EdgeDirection.Out)
      val xEdges = neighbors
        .flatMap(n1 => {
          n1._2.flatMap(n2 => n1._2.map(n3 => Edge(n3._1, n2._1, calculateDistance(n3._2, n2._2))))
        }).filter(x => x.srcId != x.dstId)
        .distinct()
      nGraph = Graph(nGraph.vertices, (nGraph.edges ++ xEdges).distinct())
      // update epsGraph
      val newEdges = xEdges.filter(_.attr >= eps)
      epsGraph =
        Graph(epsGraph.vertices, (epsGraph.edges ++ newEdges).distinct())
      // Shrink nGraph
      val nodesToRemove = epsGraph
        .collectNeighborIds(EdgeDirection.Either)
        .filter(x => x._2.length >= Mmax)
        .map(_._1).distinct()
        .collect

      //FIXME: we propabily should filter edges as well
      val numberOfNodes = nGraph.vertices.count

      nGraph = Graph(
        nGraph.vertices.filter(n => !nodesToRemove.contains(n._1)),
        nGraph.edges.filter(
          e =>
            !(nodesToRemove.contains(e.srcId) || nodesToRemove
              .contains(e.dstId))
        )
      )
      val delta = numberOfNodes - nGraph.vertices.count
      // the paper uses a AND insteat of OR but it seams wrong
      terminate = (nGraph.vertices.count < TN * docsCount && delta < TR * docsCount) || nGraph.vertices.count <= 0
      if (!terminate) {
        // FIXME: this could crash if there are not enough edges
        var remainingEdges = sc.parallelize(
          nGraph.edges.takeOrdered(nGraph.edges.count.toInt - k)(
            Ordering[Double].reverse.on(_.attr)
          )
        )
        nGraph = Graph(nGraph.vertices, remainingEdges)

      }
    }

    println("Terminated")
    println(epsGraph.edges.count)
    epsGraph.vertices.foreach(x => println(x._1 + " " + x._2))
    epsGraph.edges.foreach(x => println(x))

    //Phase 2
  }

  def calculateDistance(record1: Record, record2: Record): Double = {
    val temp =
      record1.weighsVector.toArray.union(record2.weighsVector.toSeq).map(_._1)
    val first = temp.map(record1.weighsVector)
    val second = temp.map(record2.weighsVector)
    return CosineSimilarity.cosineSimilarity(first, second)
  }

  /*
  def maxCoreNode (neighs: mutable.Set[Record]): Record = {
    var max = 0
    var ret: Record = neighs.toVector(0) //inizializzato con un valore a  caso
    neighs.foreach(x => if (x.epsNeighbors.size > max){ ret = x; max = x.epsNeighbors.size})
    return ret
  }
 */
}
