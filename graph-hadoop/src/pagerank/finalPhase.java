import java.io.IOException;
import java.util.*;
import java.lang.StringBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class finalPhase extends Configured implements Tool{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] pair = value.toString().split("\t");
			
			String[] parts = pair[1].split(":");
			output.collect(new Text(pair[0]), new Text(parts[0]));
			
		}
		
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(key,values.next());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(finalPhase.class);

		conf.setJobName("finalPhase");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}
}
