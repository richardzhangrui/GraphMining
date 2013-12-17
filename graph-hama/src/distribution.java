/* This distribution.class is mainly adapted from the hama pagerank example. 
 * I changed the main logic to compute the degree distribution */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class distribution {

  public static class vertex extends
      Vertex<Text, NullWritable, DoubleWritable> {

    @Override
    public void setup(Configuration conf) {
     
    }

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
    	
    	if (this.getSuperstepCount() == 0) {
            this.setValue(new DoubleWritable(1.0));
        }
    	else {
	    	int sum = 0;
	    	for (DoubleWritable msg: messages) {
	    		sum += msg.get();
	    		this.setValue(new DoubleWritable(sum));
	    	}
	    	voteToHalt();
	        return;
    	}
    	
    	sendMessageToNeighbors(this.getValue());
    }

  }

  public static class SeqReader
      extends
      VertexInputReader<Text, TextArrayWritable, Text, NullWritable, DoubleWritable> {
    @Override
    public boolean parseVertex(Text key, TextArrayWritable value,
        Vertex<Text, NullWritable, DoubleWritable> vertex) throws Exception {
      vertex.setVertexID(key);

      for (Writable v : value.get()) {
        vertex.addEdge(new Edge<Text, NullWritable>((Text) v, null));
      }

      return true;
    }
  }

  public static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {
    GraphJob pageJob = new GraphJob(conf, distribution.class);
    pageJob.setJobName("Distribution");
    
    pageJob.setJar("graph.jar");
    
    pageJob.setVertexClass(vertex.class);
    pageJob.setInputPath(new Path(args[0]));
    pageJob.setOutputPath(new Path(args[1]));

    pageJob.setMaxIteration(30);
    
    if (args.length == 3) {
      pageJob.setNumBspTask(Integer.parseInt(args[2]));
    }

    pageJob.setAggregatorClass(AverageAggregator.class);

    pageJob.setVertexInputReaderClass(SeqReader.class);

    pageJob.setVertexIDClass(Text.class);
    pageJob.setVertexValueClass(DoubleWritable.class);
    pageJob.setEdgeValueClass(NullWritable.class);

    pageJob.setInputFormat(SequenceFileInputFormat.class);

    pageJob.setPartitioner(HashPartitioner.class);
    pageJob.setOutputFormat(TextOutputFormat.class);
    pageJob.setOutputKeyClass(Text.class);
    pageJob.setOutputValueClass(IntWritable.class);
    return pageJob;
  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output> [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length < 2)
      printUsage();

    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    GraphJob pageJob = createJob(args, conf);

    long startTime = System.currentTimeMillis();
    if (pageJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}