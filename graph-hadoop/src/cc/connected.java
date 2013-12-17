/* The code is adapted mainly from https://github.com/wangzuo/pagerank-hadoop.
 * I changed the main logic to compute the weakly connected component.
*/

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class connected {

	public static void main(String[] args) throws Exception {
		if(args.length<5)
		{
			System.out.println("Usage:<Input><Output><Separator><InterFileName><Iteration>");
			System.exit(1);
		}
		int loop = Integer.parseInt(args[4]);
		String strs[] = new String[3];	
		strs[0] = args[0];
		strs[1] = args[3]+"-r-0";
		strs[2] = args[2];
		long start = System.currentTimeMillis();
		ToolRunner.run(new Configuration(), new initPhase(), strs);
		int cnt = 0;
		while (cnt < loop) {
			strs[0] = args[3]+"-r-"+cnt;
			cnt++;		
			strs[1] = args[3]+"-r-"+cnt;
			ToolRunner.run(new Configuration(), new computePhase(), strs);
		}
		strs[0] = args[3]+"-r-"+cnt;
		strs[1] = args[1];
		ToolRunner.run(new Configuration(), new finalPhase(), strs);
		System.out.println("Job Finished At: "+ (System.currentTimeMillis()-start));

	}
}
