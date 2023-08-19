import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Partition {

  val depth = 6

  def main ( args: Array[ String ] ) {
    val var_1 = new SparkConf().setAppName("Partition")
    val var_2 = new SparkContext(var_1)
    var summation = 0

    /* read graph from args(0); the graph cluster ID is set to -1 except for the first 5 nodes */
    var graph: RDD[(Long,Long,List[Long])] = var_2.textFile(args(0)).map(n => n.split(","))
                    .map(n => n.map(_.toLong))
                    .map(n => if (summation < 5) {summation = summation + 1;(n(0), n(0), n.drop(1).toList)} 
                              else {summation = summation + 1;(n(0), -1.toLong, n.drop(1).toList)})

    for(i <- 1 to depth){
      graph = graph.flatMap( map => map match{ case (p, q, pq) => (p, q) :: pq.map(r => (r, q) ) })
             .reduceByKey(_ max _)
             .join(graph.map(z => (z._1, (z._2, z._3))))
             .map(z => function_partition(z))
  }
      /* finally, print the partition sizes */

    graph.map(l => (l._2, 1)).reduceByKey((l, m) => (l + m)).collect().foreach(println)

    def function_partition(val_id: (Long, (Long, (Long, List[Long])))): (Long, Long, List[Long]) = {
        var var_3 = (val_id._1, val_id._2._1, val_id._2._2._2)
        if (val_id._1 != -1 && val_id._2._2._1 == -1) {
             var_3 = (val_id._1, val_id._2._1, val_id._2._2._2)
        }
        if (val_id._1 != -1 && val_id._2._2._1 != -1) {
             var_3 = (val_id._1, val_id._2._2._1, val_id._2._2._2)
        }
        var_3
    }

  }
}
