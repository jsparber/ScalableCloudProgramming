import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.graphx.{VertexRDD, EdgeDirection, Edge, Graph}
import scala.util.Random
import org.apache.spark.graphx.VertexId
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
    var gGraph = gGraphWithNoise.filter(
      graph => {
        graph.mapVertices((vid, n) => n.state == Core || n.state == Border)
      },
      vpred = (vid: VertexId, n:Boolean) => n
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
    : Graph[Record, Double] = Graph(gGraph.vertices, emptyEdges) //temporaneo
    //probabilmente in tutte le strutture dati non ha senso mettere il vero valore della distanza
    while (gGraph.vertices.filter({case (n, record) =>
      record.active
    }).count() > 0) {

    //Max selection step
    val maxCoreNodeGGraph = calcMaxCoreNodes(gGraph, true)

    val hGraphEdges = maxCoreNodeGGraph.vertices.flatMap({ case (n, record) =>
      val nmax = record.maxCoreNodes.maxBy(a => a._2)._1
      if (record != Core)
        Array(Edge(n, nmax, 1.0),
      Edge(nmax, nmax, 1.0))
      else
        record.maxCoreNodes.map(v => Edge(v._1, nmax, 1.0))
      })

    val hGraph = Graph(gGraph.vertices, hGraphEdges)

    //Pruning step
    val maxCoreNodeHGraph = calcMaxCoreNodes(hGraph, false)
    val tGraphEdges = maxCoreNodeHGraph.vertices.flatMap({ case (n, record) =>
      val nmax = record.maxCoreNodes.maxBy(a => a._2)._1
      if (record != Core)
        Array(Edge(nmax, n, 1.0))
      else {
        if (record.maxCoreNodes.exists(x => x._1 != n))
          Array(Edge(nmax, n, 1.0))
          else 
            Array[Edge[Double]]()
        }
      })

    tGraph = Graph(gGraph.vertices, tGraph.edges ++ tGraphEdges)

    val gGraphEdges = maxCoreNodeHGraph.vertices.flatMap({ case (n, record) =>
      val nmax = record.maxCoreNodes.maxBy(a => a._2)._1
      if (record != Core)
          Array[Edge[Double]]()
      else
        if (record.maxCoreNodes.length > 1)
        record.maxCoreNodes.filter(_._1 != nmax).flatMap(v => Array(Edge(v._1, nmax, 1.0), Edge(v._1, nmax, 1.0)))
          else 
            Array[Edge[Double]]()
      })

    val updateVertexs = maxCoreNodeHGraph.vertices.map({ case (n, record) =>
        val nmax = record.maxCoreNodes.maxBy(a => a._2)._1
        if (record != Core)
          record.active = false
        else {
          if (record.maxCoreNodes.exists(x => x._1 != n))
            record.active = false
          if (IsSeed(n))
            record.active = false
        }
        (n,  record)
      })

      // Build updated gGraph
      gGraph = Graph(updateVertexs, gGraphEdges)

    }
  }

  def IsSeed(n: Long) : Boolean = {
    false
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
  def calcMaxCoreNodes(
    gGraph: Graph[Record, Double],
    include_self: Boolean
  ): Graph[Record, Double] = {
      //Max selection step
      // Create a graph where the attribute is a degree of a vertex
      val graphWithDegrees = gGraph.outerJoinVertices(gGraph.degrees) { (_, _, optDegree) =>
        optDegree.getOrElse(0)
     }

      // Each vertex sends its degree to its neighbours
      // and we aggregate them in a set where each vertex gets all values
      // of its neighbours and store the node with a higher degree
      // returns a RDD[(VertexId, Int)]
      val maxCoreNeighbours = graphWithDegrees.aggregateMessages[List[(VertexId, Int)]](
        sendMsg = triplet => {
          val srcDegree = List((triplet.srcId, triplet.srcAttr))
          val dstDegree = List((triplet.dstId, triplet.dstAttr))
          triplet.sendToDst(srcDegree)
          triplet.sendToSrc(dstDegree)
        },
        mergeMsg = (x, y) => x ++ y
      )

    // Consider the node itself as possible maxCoreNode
    val maxCoreNode = if (include_self) {
      maxCoreNeighbours.innerJoin(graphWithDegrees.vertices)((vid, nDegreeList, myDegree) => {
        ((vid, myDegree)::nDegreeList).distinct
      })
    } else {
      maxCoreNeighbours.mapValues(_.distinct)
    }

    // update record
    val maxCoreNodeGraph = gGraph.outerJoinVertices(maxCoreNode) { (_, rec, optMaxDegree) =>
    rec.maxCoreNodes = optMaxDegree.getOrElse(List())
      rec
    }
    maxCoreNodeGraph
  }
}
