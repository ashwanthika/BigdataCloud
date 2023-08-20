The projects encompass a diverse range of tools and technologies for large-scale data processing and analysis. Leveraging Apache Spark, these projects employ its powerful distributed computing framework to manage and manipulate data efficiently. They utilize Spark's DataFrame API and SQL capabilities for data manipulation and processing. Additionally, the projects exhibit the application of MapReduce, a programming model for parallel processing, to execute complex computations in a distributed environment. The usage of GraphX, a graph processing library within Spark, underscores the focus on graph analysis and partitioning. Overall, the projects demonstrate the adept utilization of these tools and technologies to tackle various data-centric challenges, from graph analysis and clustering to matrix multiplication and social media data processing.
1. **Graph.scala:**
   This Scala code employs Apache Spark's DataFrame and SQL capabilities to analyze graph data stored in a CSV file. It calculates the number of neighbors for each node and groups nodes by their neighbor count, providing insights into the graph's structural properties.

2. **GraphPartition.java:**
   In this Java program, a Breadth-First Search (BFS) graph partitioning approach is implemented using MapReduce. Mapper and reducer classes traverse the graph, assigning clusters based on BFS depth. The program calculates cluster sizes using parallel processing techniques.

3. **KMeans.scala:**
   This Scala script showcases K-Means clustering using Apache Spark. It reads initial centroids from a file, iteratively assigns data points to clusters, and updates centroids. The final centroid positions are printed, demonstrating the algorithm's convergence.

4. **Multiply.java:**
   Using MapReduce, this Java code carries out matrix multiplication. It defines mapper and reducer classes to process input matrices, perform matrix multiplication, and output the resulting matrix. The code demonstrates parallel processing of matrix operations.

5. **Partition.scala:**
   This Scala program utilizes GraphX in Apache Spark to partition a graph. By applying the Pregel API, it constructs a graph, performs partitioning, and calculates the sizes of resulting partitions. The output showcases the distribution of nodes among clusters.

6. **Partition2.scala:**
   Similar to the previous code, this Scala script leverages GraphX for graph partitioning. It constructs a graph, applies partitioning, and computes partition sizes using the Pregel API. The output provides insights into the partitioning results.

7. **Twitter.java:**
   Analyzing Twitter data with MapReduce, this Java code comprises two sets of mapper and reducer classes. The first set counts followers for each user, while the second set calculates the frequency of follower counts. The final output reveals the distribution of follower frequencies.

8. **graph.pig:**
   This Pig script processes graph data by loading it from a file, determining the number of neighbors for each node, and grouping nodes based on their neighbor count. The resulting frequency distribution of neighbor counts sheds light on the connectivity patterns within the graph.
