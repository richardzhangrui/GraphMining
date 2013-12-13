import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class connected {

	/**
	 * @param args
	 */
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
			
			stat.execute("drop table if exists labels;");
			stat.execute("create table labels(id Integer, label Integer);");
			stat.execute("insert into labels select src as id,src as label from "+
			"(select src from edge union select dst from edge) as t;");
						
			int loop = Integer.parseInt(args[0]);
			
			stat.execute("drop table if exists templabels;");
			
			for(int i = 0;i < loop; i++) {
				System.out.println("loop "+i);
				stat.executeUpdate("create table templabels as " + 
						"(select edge.src as id,min(labels.label) as label from edge join labels on edge.dst=labels.id group by edge.src);");
				
				stat.executeUpdate("update labels set label = templabels.label from templabels where labels.id = templabels.id and labels.label > templabels.label;");
				stat.execute("drop table templabels;");
			}
			
			long end = System.currentTimeMillis();
			
			System.out.println("time:"+(end-begin)+" ms");
			
//			ResultSet rs = stat.executeQuery("select * from labels");
//			System.out.println("ID"+"\t"+"Label");
//			while(rs.next()) {
//				
//				System.out.println(rs.getInt(1)+"\t"+rs.getInt(2));
//				
//			}
//			rs.close();
//			
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

}
