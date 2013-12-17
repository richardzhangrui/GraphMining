/* This distribution.class is mainly adapted from the hama pagerank example. 
 * I changed some erroneous parts in the example to let it run properly */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class PageRank {

  public static class PageRankVertex extends
      Vertex<Text, NullWritable, DoubleWritable> {

    static double DAMPING_FACTOR = 0.85;
    static double MAXIMUM_CONVERGENCE_ERROR = 0.001;
    static int ITERATION = 10;
    
    static String separator = " ";

    @Override
    public void setup(Configuration conf) {
      String val = conf.get("hama.pagerank.alpha");
      if (val != null) {
        DAMPING_FACTOR = Double.parseDouble(val);
      }
      val = conf.get("hama.graph.max.convergence.error");
      if (val != null) {
        MAXIMUM_CONVERGENCE_ERROR = Double.parseDouble(val);
      }
      val = conf.get("iteration");
      if (val != null) {
    	  ITERATION = Integer.parseInt(val);
      }
    }

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
      if (this.getSuperstepCount() == 0) {
        this.setValue(new DoubleWritable(1.0 / this.getNumVertices()));
      } else if (this.getSuperstepCount() >= 1) {
        double sum = 0;
        for (DoubleWritable msg : messages) {
          sum += msg.get();
        }
        double alpha = (1.0d - DAMPING_FACTOR) / this.getNumVertices();
        this.setValue(new DoubleWritable(alpha + (sum * DAMPING_FACTOR)));
      }

      DoubleWritable globalError = getLastAggregatedValue(0);
      if (globalError != null && this.getSuperstepCount() >= ITERATION ) {
        voteToHalt();
        return;
      }

      sendMessageToNeighbors(new DoubleWritable(this.getValue().get()
          / this.getEdges().size()));
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
    	  vertex.addEdge(new Edge<Text, NullWritable>((Text)v, null));
      }
      return true;
    }
  }
  

  public static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {
    GraphJob pageJob = new GraphJob(conf, PageRank.class);
    pageJob.setJobName("Pagerank");

    pageJob.setJar("graph.jar");
    
    pageJob.setVertexClass(PageRankVertex.class);
    pageJob.setInputPath(new Path(args[0]));
    pageJob.setOutputPath(new Path(args[1]));

    pageJob.setMaxIteration(30);
    pageJob.set("hama.pagerank.alpha", "0.85");

    pageJob.set("hama.graph.self.ref", "true");
    pageJob.set("hama.graph.max.convergence.error", "0.001");
    
    pageJob.set("iterations",args[2]);
    
    if (args.length == 4) {
      pageJob.setNumBspTask(Integer.parseInt(args[3]));
    }

    pageJob.setAggregatorClass(AverageAggregator.class);

    pageJob.setVertexInputReaderClass(PagerankSeqReader.class);

    pageJob.setVertexIDClass(Text.class);
    pageJob.setVertexValueClass(DoubleWritable.class);
    pageJob.setEdgeValueClass(NullWritable.class);
    
    pageJob.setInputFormat(SequenceFileInputFormat.class);

    pageJob.setPartitioner(HashPartitioner.class);
    pageJob.setOutputFormat(TextOutputFormat.class);
    pageJob.setOutputKeyClass(Text.class);
    pageJob.setOutputValueClass(DoubleWritable.class);
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
