/* The code is adapted mainly from https://github.com/wangzuo/pagerank-hadoop.
 * I changed the main logic to compute the weakly connected component.
*/
import java.io.IOException;
import java.util.*;
import java.lang.StringBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class initPhase extends Configured implements Tool{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		
		private static String separator = " ";
		
		public void configure(JobConf conf) {
			separator = conf.get("separator");
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String nodes[] = value.toString().split(separator);
			
			Text k = new Text(nodes[0]);
			Text v = new Text(nodes[1]);
			
			output.collect(k, v);
			output.collect(v, k);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			StringBuffer sb = new StringBuffer(key.toString());
			sb.append(":");
			sb.append(key.toString());
			sb.append(",");
			
			while(values.hasNext()) {
				sb.append(values.next().toString());
				sb.append(",");
			}
			
			sb.deleteCharAt(sb.length()-1);
			
			output.collect(key, new Text(sb.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(initPhase.class);
		
		conf.set("separator", args[2]);
		
		conf.setJobName("initPhase");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}
}
