import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Graph {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val inputPath = args(0)

    // Read the input file as a DataFrame
    val var_edge = spark.read.format("csv").option("header", "false").option("delimiter", ",").option("inferSchema", "true").load(inputPath).toDF("source", "destination")

    // The number of neighbours of each node is identified 
    val neigh = var_edge.groupBy("source").count()

    // The dataframe is stored in a table inorder to make spark sql queries 
    neigh.createOrReplaceTempView("neigh")

    // The nodes in each group is computed by using sql spark queries using select, count, orderby and groupby
    val nodes_group_count = spark.sql("SELECT count(*) as numNodes, count as numNeighbors FROM neigh GROUP BY count ORDER BY count")
    nodes_group_count.rdd.map(row => (row.getLong(1), row.getLong(0)))
      .sortByKey()
      .collect()
      .foreach { case (count, nodeCount) => println(s"$count\t$nodeCount") }

    spark.stop()
}
}