import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object KMeans {
  type Point = (Double,Double)

  /* return the Euclidean distance between x and y */
  def dist(point:Point,centroid:Point) = {
      var distance_bet_points:Double =0
      distance_bet_points = math.sqrt(math.pow((point._1-centroid._1),2) + math.pow((point._2-centroid._2),2))
      distance_bet_points
  }


  def main(args: Array[ String ]) {

    var centroids: Array[Point] = Array[Point]() /* read the initial centroids from file */
    val val1 = new SparkConf().setAppName("MyJob")
    val val2 = new SparkContext(val1)
    centroids = val2.textFile(args(1)).map(line => { val cen = line.split(",")
                                     (cen(0).toDouble, cen(1).toDouble).asInstanceOf[Point]}).collect()

    for ( i <- 1 to 5 ){
       /* broadcast centroids to all workers */
      val val3 = val2.broadcast(centroids)
      /* read the dataset of points from file */
      val points:RDD[Point] = val2.textFile(args(0)).map(p => {val a = p.split(",")
                  (a(0).toDouble,a(1).toDouble)})
       /* find new centroids using KMeans */            
      centroids = points.map(p=>( closest_centroid(p,val3.value), p)).groupByKey().map(v => clusterKMeans(v._2)).collect()
    }
    centroids.foreach { case (x,y) => println(f"\t$x%2.2f\t$y%2.2f")}
  }

/* return a point c from cs that has the minimum distance(p,c) */
  def closest_centroid( p: Point, val3: Array[Point] )= val3.minBy(dist(p,_))

  def clusterKMeans(points:Iterable[Point]) = {
      var coodrinateX:Double = 0
      var coodrinatey:Double = 0
      val cluster_size = points.size
      for(point:Point <- points){
          coodrinateX = coodrinateX + point._1
          coodrinatey = coodrinatey + point._2
      }
      (coodrinateX/cluster_size,coodrinatey/cluster_size).asInstanceOf[Point]
  }
}