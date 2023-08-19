import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Triple implements Writable {
        public short tag;
        public int index;
        public float value;
      Triple(){
        }
        Triple(short val_a, int val_b, float val_c){
            this.tag = val_a;
            this.index = val_b;
            this.value = val_c;
        }
        public void write (DataOutput print_out) throws IOException{
            print_out.writeShort(tag);
            print_out.writeInt(index);
            print_out.writeFloat(value);
        }    
        public void readFields (DataInput print_inp) throws IOException{
            tag = print_inp.readShort();
            index = print_inp.readInt();
            value = print_inp.readFloat();
        }
}

class Pair implements WritableComparable<Pair> {
        public int i;
        public int j;
        Pair(){
        }
        Pair(int i, int j){
            this.i = i;
            this.j = j;
        }
        public void readFields (DataInput input) throws IOException{
            i = input.readInt();
            j = input.readInt();
        }
        public void write (DataOutput output) throws IOException{
            output.writeInt(i);
            output.writeInt(j);
        }
        public int compareTo(Pair pair) {
             if (i > pair.i) {
                return 1;
            } 
            else if (i < pair.i) {
                    return -1;
            } 
            else {
                if (j < pair.j) {
                    return -1;
                } 
                else if (j > pair.j) {
                    return 1;
                }
            }
            
            return 0;
        }

    @Override
    public String toString() { return String.format("(%d,%d)",i,j); }

}

public class Multiply extends Configured implements Tool {

    static Vector<Triple> first_value_matrix = new Vector<Triple>();
        static Vector<Triple> second_value_matrix = new Vector<Triple>();
        //first mapper for matrix
        public static class first_map_fun extends Mapper<Object, Text, IntWritable, Triple> {
            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                Scanner scan_one = new Scanner(value.toString()).useDelimiter(",");
            int i_value = scan_one.nextInt();
            int k_value = scan_one.nextInt();
            float m_value = scan_one.nextFloat();
                context.write(new IntWritable(k_value), new Triple((short)0, i_value, m_value));
                scan_one.close();
        }
        }
        //second mapper for matrix 
        public static class second_map_fun extends Mapper<Object, Text, IntWritable, Triple> {
            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scan_two = new Scanner(value.toString()).useDelimiter(",");
            int k_val = scan_two.nextInt();
            int j_val = scan_two.nextInt();
            float n_val = scan_two.nextFloat();
            context.write(new IntWritable(k_val), new Triple((short)1, j_val, n_val));
            scan_two.close();
        }
        }
        //first reducer that for mapper 1 and 2 
        public static class first_reducer extends Reducer<IntWritable,Triple,Pair,FloatWritable> {
            @Override
        public void reduce ( IntWritable index, Iterable<Triple> values, Context context ) throws IOException, InterruptedException {
            first_value_matrix.clear();
            second_value_matrix.clear();
            for (Triple value: values){
                    if (value.tag == 0){
                        first_value_matrix.add(new Triple((short)0, value.index, value.value));
                    }
                    else {
                        second_value_matrix.add(new Triple((short)1, value.index, value.value));
                        }
                    }
                for (Triple m : first_value_matrix){
                            for(Triple n : second_value_matrix) {
                                float val=m.value*n.value;
                            context.write(new Pair(m.index, n.index),new FloatWritable(val));					
                    }
                }
            }
        }
        //mapper that emits values 
        public static class final_mapper extends Mapper<Object, FloatWritable, Pair, FloatWritable> {
            public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            Scanner sc = new Scanner(values.toString()).useDelimiter("\\s+");
            int a = sc.nextInt();
                int b = sc.nextInt();
                float val = sc.nextFloat();
                Pair pair = new Pair(a, b);
            context.write(pair, new FloatWritable(val));
                }
        }
        //reducer that sums the values
        public static class final_reducer extends Reducer<Pair, FloatWritable, Text, FloatWritable> {
            @Override
            public void reduce(Pair pair, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
                float productSum = 0;
            for (FloatWritable value : values) {
                productSum = productSum + value.get();
            }
            context.write(new Text("("+(String.valueOf(pair.i)+","+String.valueOf(pair.j)+")")), new FloatWritable(productSum));
        }
        }
        @Override
        public int run ( String[] args ) throws Exception {
            /* ... */
            return 0;
        }
    
        public static void main(String[] args) throws Exception {
            //first_job configurations
                Configuration conf = new Configuration();
            Job first_job = Job.getInstance(conf, "first_job");
            first_job.setJarByClass(Multiply.class);
            first_job.setMapOutputKeyClass(IntWritable.class);
            first_job.setMapOutputValueClass(Triple.class);
            MultipleInputs.addInputPath(first_job, new Path(args[0]), TextInputFormat.class, first_map_fun.class);
                MultipleInputs.addInputPath(first_job, new Path(args[1]), TextInputFormat.class, second_map_fun.class);
            first_job.setReducerClass(first_reducer.class);
                first_job.setOutputKeyClass(Pair.class);
            first_job.setOutputValueClass(FloatWritable.class);
                first_job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(first_job, new Path(args[2]));
                first_job.waitForCompletion(true);
            
            //second_job configurations
                Job second_job = Job.getInstance(conf, "second_job");
            second_job.setJarByClass(Multiply.class);
            second_job.setMapperClass(final_mapper.class);
            second_job.setReducerClass(final_reducer.class);			
            second_job.setMapOutputKeyClass(Pair.class);
            second_job.setMapOutputValueClass(FloatWritable.class);
            second_job.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.addInputPath(second_job, new Path(args[2]));
            second_job.setOutputFormatClass(TextOutputFormat.class);
                FileOutputFormat.setOutputPath(second_job, new Path(args[3]));
            second_job.waitForCompletion(true);
        }
    }