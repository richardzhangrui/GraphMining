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
				String label = parts[0];
				String node = parts[1];
				String[] nodes = node.split(",");
								
				int cnt = nodes.length;
				for(int i = 0; i < cnt; i++) {					
					output.collect(new Text(nodes[i]), new Text(label+","+key));
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String pairs[] = values.next().toString().split(",");
			
			int min = Integer.parseInt(pairs[0]);
							
			StringBuffer nodes = new StringBuffer(pairs[1]);
			nodes.append(",");
			
			
			while (values.hasNext()) {
				String value = values.next().toString();
				
				pairs = value.split(",");
				
				int label = Integer.parseInt(pairs[0]);
				
				if(label < min)
					min = label;	
				
				nodes.append(pairs[1]);
				nodes.append(",");
				
			}
			
			if(nodes.length()>0) {
				nodes.deleteCharAt(nodes.length()-1);
			}
			
			StringBuffer v = new StringBuffer("");
			v.append(min);
			v.append(":");
			
			v.append(nodes);
			
			output.collect(key, new Text(v.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(computePhase.class);

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