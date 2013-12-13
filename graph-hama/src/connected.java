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

/**
 * Real pagerank with dangling node contribution.
 */
public class connected {

  public static class PageRankVertex extends
      Vertex<Text, NullWritable, DoubleWritable> {

    static int ITERATION = 7;

    @Override
    public void setup(Configuration conf) {
      
      String val = conf.get("iteration");
      if (val != null) {
    	  ITERATION = Integer.parseInt(val);
      }
    }

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
      // initialize this vertex to 1 / count of global vertices in this graph
      if (this.getSuperstepCount() == 0) {
        this.setValue(new DoubleWritable(Double.parseDouble(this.getVertexID().toString())));
      } else if (this.getSuperstepCount() >= 1) {
        double min = this.getValue().get();
        for (DoubleWritable msg : messages) {
        	double v = msg.get();
        	if (v<min)
        		min = v;
        }

        this.setValue(new DoubleWritable(min));
      }
      if (this.getSuperstepCount() >= ITERATION ) {
        voteToHalt();
        return;
      }

      // in each superstep we are going to send a new rank to our neighbours
      sendMessageToNeighbors(this.getValue());
    }

  }

  public static class PagerankSeqReader
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
    GraphJob pageJob = new GraphJob(conf, PageRank.class);
    pageJob.setJobName("Pagerank");

    pageJob.setVertexClass(PageRankVertex.class);
    pageJob.setInputPath(new Path(args[0]));
    pageJob.setOutputPath(new Path(args[1]));

    pageJob.setJar("graph.jar");
    
    // set the defaults
    pageJob.setMaxIteration(30);
    // reference vertices to itself, because we don't have a dangling node
    // contribution here    
    pageJob.set("iterations",args[2]);
    
    if (args.length == 4) {
      pageJob.setNumBspTask(Integer.parseInt(args[3]));
    }

    // error
    pageJob.setAggregatorClass(AverageAggregator.class);

    // Vertex reader
    pageJob.setVertexInputReaderClass(PagerankSeqReader.class);

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
    System.out.println("Usage: <input> <output> <iterations> [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length < 3)
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