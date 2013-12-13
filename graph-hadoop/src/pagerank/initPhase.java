import java.io.IOException;
import java.util.*;
import java.lang.StringBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class initPhase extends Configured implements Tool{

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
			output.collect(value, key);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static double n;
		public void configure(JobConf conf) {
			n = Double.parseDouble(conf.get("n"));
		}
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			double pagerank = 1.0/n;
			StringBuffer v = new StringBuffer(String.valueOf(pagerank));
			v.append(":");
			while (values.hasNext()) {
				v.append(values.next().toString());
				v.append(",");
			}
			v.deleteCharAt(v.length()-1);
			output.collect(key, new Text(v.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(initPhase.class);

		conf.set("n", args[2]);

		conf.setJobName("initPhase");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.set("key.value.separator.in.input.line",args[3]);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}
}
