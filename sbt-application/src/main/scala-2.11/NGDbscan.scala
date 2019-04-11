import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.graphx.{VertexRDD, EdgeDirection, Edge, Graph}
import scala.util.Random
import State._

object ngDBSCAN {
  val eps = 0.15
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
    val nodes = records.zipWithUniqueId().map(_.swap)
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
          n1._2.flatMap(
            n2 =>
              n1._2
                .map(n3 => Edge(n3._1, n2._1, calculateDistance(n3._2, n2._2)))
          )
        })
        .filter(x => x.srcId != x.dstId)
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
        .map(_._1)
        .distinct()
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
      println("With filter" + nGraph.edges.count)
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
    //println(epsGraph.edges.count)
    //epsGraph.vertices.foreach(x => println(x._1 + " " + x._2))
    //epsGraph.edges.foreach(x => println(x.attr))
    //println(toGexf(epsGraph))

    //Phase 2
    println("Phase 2");
    val coresDegrees = epsGraph.degrees.filter(x => x._2 >= minPts)
    println(epsGraph.degrees.max()._2)
    val coreGraph = epsGraph.joinVertices(coresDegrees)((_, rec, _) => {
      rec.state = Core; rec
    })
    val borderness = coreGraph
      .collectNeighbors(EdgeDirection.Out)
      .map({
        case (vid, a) => (vid, a.map(_._2.state == Core).fold(false)(_ || _))
      })
    val gGraphWithNoise = coreGraph.joinVertices(borderness)((_, rec, arg) => {
      if (arg && rec.state != Core)
        rec.state = Border
      rec
    })

    // Filter noise
    var gGraph = Graph(
      gGraphWithNoise.vertices
        .filter(n => n._2.state == Core || n._2.state == Border),
      gGraphWithNoise.edges
    )
    println("Number of nodes" + gGraph.vertices.count())
    println(
      "Number of core nodes" + gGraph.vertices
        .filter(x => x._2.state == Core)
        .count
    )
    println(
      "Number of border nodes" + gGraph.vertices
        .filter(x => x._2.state == Border)
        .count
    )
    var tGraph
        : Graph[Record, Double] = Graph(gGraph.vertices, null) //temporaneo
    //probabilmente in tutte le strutture dati non ha senso mettere il vero valore della distanza
    while (gGraph.vertices.count() > 0) {

      //Max selection step

      //i nodi dell'Hgrafo sono gli stessi del Ggrafo
      var hGraph: Graph[Record, Double] = Graph(gGraph.vertices, null)
      var huh: Int = hGraph.co
      gGraph.vertices.map(
        x =>
          x._2.mostCorenessNeighbor = maxCoreNode(
            sc.parallelize(
                gGraph
                  .collectNeighbors(EdgeDirection.Out)
                  .filter(y => y._1 == x._1)
                  .first()
                  ._2
              )
              .union(sc.parallelize(Seq(x))),
            gGraph
          )
      )
      gGraph.vertices.foreach(
        x =>
          if (x._2.state != Core) {
            hGraph.edges ++= EdgeRDD(
              x._1,
              x._2.mostCorenessNeighbor._1,
              calculateDistance(x._2, x._2.mostCorenessNeighbor._2)
            )
            hGraph.edges ++= EdgeRDD(
              x._2.mostCorenessNeighbor._1,
              x._2.mostCorenessNeighbor._1,
              1
            )
          } else {
            sc.parallelize(
                gGraph
                  .collectNeighbors(EdgeDirection.Out)
                  .filter(y => y._1 == x._1)
                  .first()
                  ._2
              )
              .union(sc.parallelize(Seq(x)))
              .foreach(
                z =>
                  hGraph.edges ++= EdgeRDD(
                    z._1,
                    x._2.mostCorenessNeighbor._1,
                    calculateDistance(z._2, x._2)
                  )
              )
          }
      )
      //Pruning step
      gGraph = Graph(gGraph.vertices, null)
      hGraph.vertices.map(
        x =>
          x._2.mostCorenessNeighbor = maxCoreNode(
            sc.parallelize(
              hGraph
                .collectNeighbors(EdgeDirection.Out)
                .filter(y => y._1 == x._1)
                .first()
                ._2
            ),
            hGraph
          )
      )
      hGraph.vertices.foreach(
        x =>
          if (x != Core) {
            //disattivare x
            tGraph.edges ++= EdgeRDD(
              x._2.mostCorenessNeighbor._1,
              x._1,
              calculateDistance(x._2.mostCorenessNeighbor._2, x._2)
            )
          } else {
            if (hGraph.vertices.count() > 1) {
              sc.parallelize(
                  gGraph
                    .collectNeighbors(EdgeDirection.Out)
                    .filter(y => y._1 == x._1)
                    .first()
                    ._2
                )
                .subtract(sc.parallelize(Seq(x._2.mostCorenessNeighbor)))
                .foreach { z =>
                  gGraph.edges ++= EdgeRDD(
                    z._1,
                    x._2.mostCorenessNeighbor._1,
                    calculateDistance(z._2, x._2.mostCorenessNeighbor._2)
                  )
                  gGraph.edges ++= EdgeRDD(
                    x._2.mostCorenessNeighbor._1,
                    z._1,
                    calculateDistance(x._2.mostCorenessNeighbor._2, z._2)
                  )
                }

            }
            if (hGraph
                  .collectNeighbors(EdgeDirection.Out)
                  .filter(y => y._1 == x._1)
                  .first()
                  ._2
                  .contains(x._1)) {
              tGraph.edges ++= EdgeRDD(
                x._2.mostCorenessNeighbor._1,
                x._1,
                calculateDistance(x._2.mostCorenessNeighbor._2, x._2)
              )
              //disattivare x
              //if isSeed (x) {disattivare x}
            }
          }
      )
    }
  }

  def calculateDistance(record1: Record, record2: Record): Double = {
    val temp =
      record1.weighsVector.toArray.union(record2.weighsVector.toSeq).map(_._1)
    val first = temp.map(record1.weighsVector)
    val second = temp.map(record2.weighsVector)
    return CosineSimilarity.cosineSimilarity(first, second)
  }

  def toGexf[VD, ED](g: Graph[VD, ED]): String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices
        .map(
          v =>
            "      <node id=\"" + v._1 + "\" label=\"" +
              v._2
                .asInstanceOf[Record]
                .tweet
                .replaceAll("\"", "")
                .replaceAll("&", "") + "\" />\n"
        )
        .collect
        .mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges
        .map(
          e =>
            "      <edge source=\"" + e.srcId +
              "\" target=\"" + e.dstId + "\" weight=\"" + e.attr +
              "\" />\n"
        )
        .collect
        .mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
  }
  def maxCoreNode(
      nodes: RDD[(VertexId, Record)],
      g: Graph[Record, Double]
  ): (VertexId, Record) = {
    val temp = Graph(nodes, g.edges)
    val maxValue = temp.degrees.max()(Ordering[Int].on(_._2))._1
    val ret = temp.vertices.filter(x => x._1 == maxValue)
    ret.first()
  }
}
