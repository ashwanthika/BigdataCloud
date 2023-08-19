import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Partition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)

    var count = 0

    // read graph from args(0); the graph cluster ID is set to -1 except for the first 5 nodes
    val G: RDD[(Long, Long, List[Long])] = sc.textFile(args(0)).map(line => {
      val a = line.split(",").toList
      val id = a.head.toLong
      val cluster = if (count < 5) id else -1
      count += 1
      (id, cluster, a.tail.map(_.toLong))
    })

    // create the edges RDD
    val edges: RDD[Edge[Int]] = G.flatMap({ case (source, _, destination) =>
      destination.map(val1 => Edge(source, val1, 0))
    })

    // create the vertices RDD
    val vertices: RDD[(VertexId, Long)] = G.map({ case (vertex_id, cluster, _) =>
      (vertex_id, cluster)
    })

    // create the GraphX graph
    val graph: Graph[Long, Int] = Graph(vertices, edges, 0L)

    // function to compute new cluster number for each vertex
    def newCluster(id: VertexId, currentCluster: Long, incomingCluster: Long): Long =
      if (currentCluster == -1) incomingCluster else currentCluster

    // function to send the vertex cluster ID to the outgoing neighbors
    def sendMessage(triplet: EdgeTriplet[Long, Int]): Iterator[(VertexId, Long)] =
      Iterator((triplet.dstId, triplet.srcAttr))

    // function to merge the incoming cluster IDs
    def mergeClusters(x: Long, y: Long): Long =
      if (x == -1) y else x

    // partition the graph using Pregel
    val prg = graph.pregel(-1L, 6)(
      newCluster,
      sendMessage,
      mergeClusters
    )

    // group the vertices by their cluster number and print the partition sizes
    prg.vertices.groupBy(_._2).mapValues(_.size).sortByKey().collect().foreach{case (key_partition, value_partition) => println(s"$key_partition\t$value_partition")}

   sc.stop()
  }
}