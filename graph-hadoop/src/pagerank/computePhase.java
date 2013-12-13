import java.io.IOException;
import java.util.*;
import java.lang.StringBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class computePhase extends Configured implements Tool{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] pair = value.toString().split("\t");
			
			String[] parts = pair[1].split(":");
			if (parts.length > 1) {
				String pgvalue = parts[0];
				String node = parts[1];
				String[] nodes = node.split(",");
				
				int cnt = nodes.length;
				for(int i = 0; i < cnt; i++) {
					StringBuffer sb = new StringBuffer(pgvalue);
					sb.append(":");
					sb.append(String.valueOf(cnt));
					sb.append(":");
					sb.append(key.toString());
					output.collect(new Text(nodes[i]), new Text(sb.toString()));
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static double n;
		public void configure(JobConf conf) {
			n = Double.parseDouble(conf.get("n"));
		}
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			double pagerank = 1.0/n;
			StringBuffer v = new StringBuffer("");
			double sum = pagerank * 0.15;
			StringBuffer nodes = new StringBuffer("");
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] parts = value.split(":");
				sum += Double.parseDouble(parts[0])/Double.parseDouble(parts[1]);
				nodes.append(parts[2]);
				nodes.append(",");
			}
			v.append(String.valueOf(sum));
			v.append(":");
			v.append(nodes);
			v.deleteCharAt(v.length()-1);
			output.collect(key, new Text(v.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(computePhase.class);

		conf.set("n", args[2]);

		conf.setJobName("computePhase");

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
