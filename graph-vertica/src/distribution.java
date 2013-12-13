import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class distribution {

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
					"jdbc:vertica://54.209.61.37:5433/test",
					prop);
			
			Statement stat = conn.createStatement();			
			
			long begin = System.currentTimeMillis();
			stat.execute("SELECT src,count(*) as cnt FROM edge group by src order by cnt desc;");
			long end = System.currentTimeMillis();
			
			System.out.println("time:"+(end-begin)+" ms");
			/*System.out.println("src"+"\t"+"cnt");
			while(rs.next()) {
				
				System.out.println(rs.getInt(1)+"\t"+rs.getInt(2));
				
			}
			rs.close();*/
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

}
