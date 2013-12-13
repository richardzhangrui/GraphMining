
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class distribution{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static String separator = " ";
		
		public void configure(JobConf conf) {
			separator = conf.get("separator");
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			String nodes[] = value.toString().split(separator);
			
			output.collect(new Text(nodes[0]), one);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			int sum = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length<1) {
			System.out.println("Usage:<input><output><separator>");
			System.exit(1);
		}
		
		JobConf conf = new JobConf(distribution.class);

		conf.setJobName("distribution");
		
		conf.set("separator", args[2]);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		long start = System.currentTimeMillis();
		JobClient.runJob(conf);
		System.out.println("Job Finished At: "+ (System.currentTimeMillis()-start));
		
	}
}