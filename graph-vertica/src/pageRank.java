import java.sql.*;
import java.util.Properties;

public class pageRank {

	
	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("user", "dbadmin");
		prop.put("password", "123456");
		
		Connection conn;
		try {
			conn = DriverManager.getConnection(
					"jdbc:vertica://54.209.65.48:5433/test",
					prop);
			
			Statement stat = conn.createStatement();
			
			long begin = System.currentTimeMillis();
			stat.execute("DROP TABLE IF EXISTS pagerank");
			stat.execute("DROP TABLE IF EXISTS edgeWithOuterDegree");

			String cmd = String.format("CREATE TABLE pagerank AS SELECT * , 1.00/%s AS pr FROM (SELECT src FROM edge " +
							  "UNION " +
							  "SELECT dst FROM edge) foo;", args[0]);
			stat.execute(cmd);
			
			System.out.println("create pagerank");
			
			stat.execute("CREATE TABLE edgeWithOuterDegree AS SELECT edge.src, edge.dst, weight.wei FROM edge JOIN (SELECT edge.src, 1.00/COUNT(edge.dst) AS wei FROM edge GROUP BY edge.src) as weight ON edge.src = weight.src;");
			
			System.out.println("create degree");
			
			int loop = Integer.parseInt(args[1]);
			
			//stat.execute("drop table if exists pagerank1");
			
			for(int i = 0; i < loop; i++) {
				
				System.out.println("loop" + i);
				
				//stat.execute("CREATE TABLE pagerank1 AS SELECT edgeWithOuterDegree.dst as src, SUM(pagerank.pr*edgeWithOuterDegree.wei*0.85) AS pr FROM pagerank LEFT JOIN edgeWithOuterDegree ON pagerank.src = edgeWithOuterDegree.src GROUP BY edgeWithOuterDegree.dst;");
				
				stat.execute("CREATE TABLE currentpagerank AS SELECT pagerank.src, 0.15/"+args[0]+"+COALESCE(pagerank1.pr,0) as pr "+
							"FROM pagerank LEFT JOIN "+
						"(SELECT edgeWithOuterDegree.dst as src, SUM(pagerank.pr*edgeWithOuterDegree.wei*0.85) AS pr FROM pagerank LEFT JOIN edgeWithOuterDegree ON pagerank.src = edgeWithOuterDegree.src GROUP BY edgeWithOuterDegree.dst) as pagerank1 ON pagerank.src = pagerank1.src;");
				//stat.execute("DROP TABLE pagerank1;");
				stat.execute("DROP TABLE pagerank;");
				stat.execute("ALTER TABLE currentpagerank RENAME TO pagerank;");
				
			}
			
			stat.execute("DROP TABLE IF EXISTS edgeWithOuterDegree;");
			
			long end = System.currentTimeMillis();
			
			System.out.println("time:"+(end-begin)+" ms");
			
//			ResultSet rs = stat.executeQuery("SELECT src,pr FROM pagerank;");
//			System.out.println("src"+"\t"+"pagerank");
//			while(rs.next()) {
//				
//				System.out.println(rs.getInt(1)+"\t"+rs.getDouble(2));
//				
//			}
//			rs.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
