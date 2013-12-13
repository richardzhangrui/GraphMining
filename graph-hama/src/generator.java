import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.sync.SyncException;

public class generator {

  private static String SIZE_OF_MATRIX = "size.of.matrix";
//  private static String DENSITY = "density.of.matrix";

  public static class SymmetricMatrixGenBSP extends
      BSP<LongWritable, Text, Text, TextArrayWritable, Text> {

    private Configuration conf;
    private int sizeN;
    private String separator;
//    private int density;
    private Map<Integer, HashSet<Integer>> list = new HashMap<Integer, HashSet<Integer>>();

    @Override
    public void setup(
        BSPPeer<LongWritable, Text, Text, TextArrayWritable, Text> peer) {
      this.conf = peer.getConfiguration();
      sizeN = conf.getInt(SIZE_OF_MATRIX, 10);
      separator = conf.get("SEPARATOR");
//      density = conf.getInt(DENSITY, 1);
    }

    @Override
    public void bsp(
        BSPPeer<LongWritable, Text, Text, TextArrayWritable, Text> peer)
        throws IOException, SyncException, InterruptedException {
      int interval = sizeN / peer.getNumPeers();
      int startID = 1 + peer.getPeerIndex() * interval;
      int endID;
      if (peer.getPeerIndex() == peer.getNumPeers() - 1)
        endID = sizeN;
      else
        endID = startID + interval;
      
//      for(int i = startID; i < endID; i++) {
//    	  list.put(i, new HashSet<Integer>());
//    	  list.get(i).add(0);
//      }
//      
//      if (peer.getPeerIndex() == 0) {
//    	  list.put(0, new HashSet<Integer>());
//    	  list.get(0).add(0);
//      }
      
      LongWritable key = new LongWritable(1);
      Text value = new Text("");
      while(peer.readNext(key,value)) {
    	  String[] nodes = value.toString().split(separator);
    	  int src = Integer.parseInt(nodes[0]);
    	  int dst = Integer.parseInt(nodes[1]);
    	  
    	  int peerIndex = (src-1) / interval;
          if (peerIndex >= peer.getNumPeers())
            peerIndex = peerIndex - 1;
    	  
          if (peerIndex == peer.getPeerIndex()) {
        	  HashSet<Integer> nList = list.get(src);
        	  if(nList == null)
        		  nList = new HashSet<Integer>();
        	  nList.add(dst);
        	  list.put(src, nList);
          } else {
        	  peer.send(peer.getPeerName(peerIndex), new Text(src + "," + dst));
          }    
          
          peerIndex = (dst-1) / interval;
          if (peerIndex >= peer.getNumPeers())
            peerIndex = peerIndex - 1;
    	  
          if (peerIndex == peer.getPeerIndex()) {
        	  HashSet<Integer> nList = list.get(dst);
        	  if(nList == null)
        		  nList = new HashSet<Integer>();
        	  nList.add(src);
        	  list.put(dst, nList);
          } else {
        	  peer.send(peer.getPeerName(peerIndex), new Text(dst + "," + src));
          } 
    	  
      }
      

      // Synchronize the upper and lower
      peer.sync();
      Text received;
      while ((received = peer.getCurrentMessage()) != null) {
        String[] kv = received.toString().split(",");
        HashSet<Integer> nList = list.get(Integer.parseInt(kv[0]));
        if(nList == null)
        	nList = new HashSet<Integer>();
        nList.add(Integer.parseInt(kv[1]));
        list.put(Integer.parseInt(kv[0]), nList);
      }
    }

    @Override
    public void cleanup(
        BSPPeer<LongWritable, Text, Text, TextArrayWritable, Text> peer)
        throws IOException {
      for (Map.Entry<Integer, HashSet<Integer>> e : list.entrySet()) {
        Writable[] values = new Writable[e.getValue().size()];
        if (values.length > 0) {
          int i = 0;
          for (Integer v : e.getValue()) {
            values[i] = new Text(String.valueOf(v));
            i++;
          }

          TextArrayWritable value = new TextArrayWritable();
          value.set(values);
          peer.write(new Text(String.valueOf(e.getKey())), value);
        }
      }
    }
  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    if (args.length < 5) {
      System.out
          .println("Usage: <size n> <separator> <input path> <output path> <number of tasks>");
      System.exit(1);
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    conf.setInt(SIZE_OF_MATRIX, Integer.parseInt(args[0]));
//    conf.setInt(DENSITY, Integer.parseInt(args[1]));
    conf.set("SEPARATOR", args[1]);

    BSPJob bsp = new BSPJob(conf);
   
    // Set the job name
    bsp.setJar("graph.jar");
    bsp.setJobName("Graph Generator");
    bsp.setBspClass(SymmetricMatrixGenBSP.class);
    bsp.setInputFormat(TextInputFormat.class);
    bsp.setInputKeyClass(LongWritable.class);
    bsp.setInputValueClass(Text.class);
    FileInputFormat.setInputPaths(bsp, new Path(args[2]));
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(TextArrayWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, new Path(args[3]));
    bsp.setNumBspTask(Integer.parseInt(args[4]));

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}