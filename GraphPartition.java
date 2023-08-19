import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

//Class vertex that contains the details for each node. 
class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    
		public Vertex() {
		}
		public Vertex(long v_Id, Vector adjacent, long centroid, short depth) {
			this.id = v_Id;
			this.adjacent = adjacent;
			this.centroid = centroid;
			this.depth = depth;
		}
        public final void readFields(DataInput input) throws IOException {

			this.id = input.readLong();
			this.centroid = input.readLong();
			this.depth = input.readShort();
			this.adjacent = readDataVector(input);
		}
		public static Vector readDataVector(DataInput input) throws IOException {
			int var=1;
			Vector<Long> vector_value = new Vector<Long>();
			Long data;
			while(var>0)
			{
				try {
					if((data=input.readLong())!= -1){
						vector_value.add(data);
				    	}
					else{
						var = 0;
					}
				}
				catch(EOFException var_end_of_file)
				{
					var=0;
				}

			}
			return vector_value;
		}
		public void write(DataOutput output) throws IOException
		{
			output.writeLong(this.id);
			output.writeLong(this.centroid);
			output.writeShort(this.depth);
			for(int var_j=0; var_j<this.adjacent.size(); var_j++){
				output.writeLong(this.adjacent.get(var_j));
			}
		}
		public int comparison(Vertex vertex)
		{
			return 0;
		}
        
}

public class GraphPartition {

//setting the starting depth for BFS as 0 and setting the max depth for the BFS depth to be 8. 
    final static short maxDepth = 8;
	public static short BFS_depth = 0;
//Mapper class that traverses the graph and its vertices. 
	public static class map_Graph extends Mapper<LongWritable, Text,LongWritable,Vertex>
	{
		private int init_c = 0;
		public void map ( LongWritable key_map, Text val_map, Context context )
				throws IOException, InterruptedException
			{
				String line = val_map.toString();
				String[] vertexIds = line.split(",");
				long v_id = Long.parseLong(vertexIds[0]);
				long centroid = -1;
				Vector<Long> v_adj = new Vector<Long>();
				for(int var=1; var<vertexIds.length; var++)
					v_adj.add(Long.parseLong(vertexIds[var]));
				if(init_c < 10) {
					centroid = v_id;
					context.write(new LongWritable(v_id),new Vertex(v_id,v_adj,centroid,(short) 0));
				}
				else{
					context.write(new LongWritable(v_id),new Vertex(v_id,v_adj,centroid,(short) 0));
				}
				init_c = init_c + 1;
			}
	}
	//mapper that performs BFS operation. 
	public static class map_BFS extends Mapper<LongWritable, Vertex, LongWritable, Vertex>
	{
		public void map ( LongWritable key_map, Vertex vertex, Context context )
				throws IOException, InterruptedException
			{
				context.write(new LongWritable(vertex.id),vertex);
				if(vertex.centroid > 0){
					for(long n : vertex.adjacent){
						context.write(new LongWritable(n), new Vertex(n, new Vector<Long>(), vertex.centroid, BFS_depth));
					}
				}
			}
	}
//Reducer for the corresponding BFS that assigns creates a new vertx M and assigns the adjacent and centroid based on the inputs received from the other two mappers.
	public static class Red_BFS extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		public void reduce ( LongWritable key_red, Iterable<Vertex> val_red, Context context )
				throws IOException, InterruptedException {
				short min_Depth = 1000;
				Vertex m = new Vertex(-1,new Vector<Long>(),-1,(short) 0);
				for(Vertex vertex :val_red)
				{
					if(!vertex.adjacent.isEmpty())
						m.adjacent = vertex.adjacent;
					if(vertex.depth < min_Depth && vertex.centroid > 0) {
						min_Depth = vertex.depth;
						m.centroid = vertex.centroid;
					}
					m.id = vertex.id;
				}
				m.depth = min_Depth;
				context.write(key_red,m);
		}
	}
	// from the values emitted from the previous reducer is passed into the mapper and assigned a count 1 
	public static class emit_map extends Mapper<LongWritable, Vertex, LongWritable, IntWritable>
	{
		public void map ( LongWritable key_map, Vertex val_map, Context context ) throws IOException, InterruptedException
			{
				context.write(new LongWritable(val_map.centroid),new IntWritable(1));
			}
	}
// the final reducer counts and gives the output. 
	public static class emit_red extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

		public void reduce ( LongWritable key_red, Iterable<IntWritable> val_red, Context context ) throws IOException, InterruptedException {
				int m = 0;
				for(IntWritable value :val_red){
					m = m + value.get();}
				context.write(key_red,new IntWritable(m));
		}
	}


    /* ... */

    public static void main ( String[] args ) throws Exception {
        Job one_first_job = Job.getInstance();
		one_first_job.setJobName("Job one");
		one_first_job.setJarByClass(GraphPartition.class);
		one_first_job.setMapperClass(map_Graph.class);
		one_first_job.setOutputKeyClass(LongWritable.class);
		one_first_job.setOutputValueClass(Vertex.class);
		one_first_job.setMapOutputKeyClass(LongWritable.class);
		one_first_job.setMapOutputValueClass(Vertex.class);
		one_first_job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(one_first_job,new Path(args[0]));
		FileOutputFormat.setOutputPath(one_first_job,new Path(args[1]+"/i0"));
		        /* ... First Map-Reduce job to read the graph */

        one_first_job.waitForCompletion(true);
		for ( short i = 0; i < maxDepth; i++ ) {
			BFS_depth += 1;
			Job two_second_job = Job.getInstance();
			two_second_job.setJobName("tempJob");
			two_second_job.setJarByClass(GraphPartition.class);
			two_second_job.setMapperClass(map_BFS.class);
			two_second_job.setReducerClass(Red_BFS.class);
			two_second_job.setOutputKeyClass(LongWritable.class);
			two_second_job.setOutputValueClass(Vertex.class);
			two_second_job.setMapOutputKeyClass(LongWritable.class);
			two_second_job.setMapOutputValueClass(Vertex.class);
			two_second_job.setInputFormatClass(SequenceFileInputFormat.class);
			two_second_job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(two_second_job,new Path(args[1]+"/i"+i));
			FileOutputFormat.setOutputPath(two_second_job,new Path(args[1]+"/i"+(i+1)));
			            /* ... Second Map-Reduce job to do BFS */
            two_second_job.waitForCompletion(true);
		}
		Job three_third_job = Job.getInstance();
		three_third_job.setJobName("JobThree");
		three_third_job.setJarByClass(GraphPartition.class);
		three_third_job.setMapperClass(emit_map.class);
		three_third_job.setReducerClass(emit_red.class);
		three_third_job.setOutputKeyClass(LongWritable.class);
		three_third_job.setOutputValueClass(IntWritable.class);
		three_third_job.setMapOutputValueClass(IntWritable.class);
		three_third_job.setMapOutputKeyClass(LongWritable.class);
		three_third_job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(three_third_job,new Path(args[1]+"/i8"));
		FileOutputFormat.setOutputPath(three_third_job,new Path(args[2]));
		        /* ... Final Map-Reduce job to calculate the cluster sizes */

        three_third_job.waitForCompletion(true);

    }

}