import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


//The class for the mapreduce program for twitter analysis is named Twitter
public class Twitter {
//The program contains two mapper and reducer classes to solve the given question.
    public static class Mapper_one_Axu extends Mapper<Object,Text,IntWritable,IntWritable>
    {
        @Override
        public void map ( Object key_map, Text value_map, Context context )
                throws IOException, InterruptedException
//      Overriding the existing map function to write the code for first mapper
        {
//          Setting the delimiting pattern of the Scanner by using the delimiter function
            String var_to_string = value_map.toString();
            Scanner read_input = new Scanner(var_to_string).useDelimiter(",");
//          Scan the upcoming values of input as int datatype and storing it in 2 variables for key and value.
            int temp_var1 = read_input.nextInt();
            int temp_var2 = read_input.nextInt();
            int var1 = temp_var1;
            int var2 = temp_var2;
            context.write(new IntWritable(var2), new IntWritable(var1));
            read_input.close();

        }
    }

    public static class Reduce_one_Axu extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
    {
        @Override
        public void reduce ( IntWritable key_reduce, Iterable<IntWritable> values_reduce, Context context )
                throws IOException, InterruptedException {
            // Overriding the existing reduce function to write the code for first reducer
            int counter = 0;
            for (IntWritable temp_val:values_reduce)
            {
                counter++;
            };
            context.write(key_reduce,new IntWritable(counter));
        }
    }

    public static class Mapper_two_Axu extends Mapper<Object,Text,IntWritable,IntWritable>
    {
        @Override
        public void map ( Object key_map, Text value_map, Context context )
                throws IOException, InterruptedException
        {
            //      Overriding the existing map function to write the code for second mapper
            //      Setting the delimiting pattern of the Scanner by using the delimiter function
            String var_to_string = value_map.toString();
            Scanner read_input = new Scanner(var_to_string).useDelimiter("\t");
            //      Scan the upcoming values of input as int datatype and storing it in 2 variables for key and value.
            int temp_var1 = read_input.nextInt();
            int temp_var2 = read_input.nextInt();
            int var_1 = temp_var1;
            int var_2 = temp_var2;
            context.write(new IntWritable(var_2), new IntWritable(1));
            read_input.close();
        }
    }

    public static class Reduce_two_Axu  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
    {
        @Override
        public void reduce ( IntWritable reduce_key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException
        {
            //      Overriding the existing reduce function to write the code for second reducer
            int temp_sum = 0;
            for (IntWritable temp_var: values) {
                temp_sum += temp_var.get();
            };
            context.write(reduce_key,new IntWritable(temp_sum));
        }
    }


    public static void main ( String[] args ) throws Exception
    {
        Job twit = Job.getInstance();
        twit.setJobName("TwitAnalysis1");
        twit.setJarByClass(Twitter.class);
        twit.setOutputKeyClass(IntWritable.class);
        twit.setOutputValueClass(IntWritable.class);
        twit.setMapOutputKeyClass(IntWritable.class);
        twit.setMapOutputValueClass(IntWritable.class);
        twit.setMapperClass(Mapper_one_Axu.class);
        twit.setReducerClass(Reduce_one_Axu.class);
        twit.setInputFormatClass(TextInputFormat.class);
        twit.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(twit,new Path(args[0]));
        FileOutputFormat.setOutputPath(twit,new Path(args[1]));
        twit.waitForCompletion(true);

        Job twit2 = Job.getInstance();
        twit2.setJobName("TwitAnalysis2");
        twit2 .setJarByClass(Twitter.class);
        twit2 .setOutputKeyClass(IntWritable.class);
        twit2 .setOutputValueClass(IntWritable.class);
        twit2 .setMapOutputKeyClass(IntWritable.class);
        twit2 .setMapOutputValueClass(IntWritable.class);
        twit2 .setMapperClass(Mapper_two_Axu.class);
        twit2 .setReducerClass(Reduce_two_Axu.class);
        twit2 .setInputFormatClass(TextInputFormat.class);
        twit2 .setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(twit2 ,new Path(args[1]));
        FileOutputFormat.setOutputPath(twit2 ,new Path(args[2]));
        System.exit(twit2.waitForCompletion(true) ? 0 : 1);
    }
    
}
