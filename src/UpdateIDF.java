import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;


public class UpdateIDF
{
	private static String USER = "cs246";
	private static String PASS = "cs246"; 
	private static String URL = "jdbc:mysql://localhost:3306/cs246";
	
	private static Connection conn;
	
	public static void main(String[] args) throws SQLException
	{
		Properties p = new Properties();
		p.put("user", USER);
		p.put("password", PASS);
		PreparedStatement selectTF;
		PreparedStatement insertIDF;
		try {
			conn = DriverManager.getConnection(URL, p);
			selectTF = conn.prepareStatement("SELECT count(*) FROM TF WHERE tid = ? AND agency = ?");
			insertIDF = conn.prepareStatement("INSERT INTO IDF VALUES(?, ?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to initialize DBManager");
		}
		
		
		DBManager dbm = new DBManager();
		ResultSet rs = dbm.executeSelect("SELECT tid FROM Terms");
		String agency = "CNN";
		while(rs.next()) {
			int tid = rs.getInt(1);
			
			selectTF.setInt(1, tid);
			selectTF.setString(2, agency);
			ResultSet count = selectTF.executeQuery();
			count.first();
			int idfCount = count.getInt(1);
			insertIDF.setInt(1, tid);
			insertIDF.setInt(2, idfCount);
			insertIDF.setString(3, agency);
			insertIDF.executeUpdate();
		}
		
		rs = dbm.executeSelect("SELECT tid FROM Terms");
		agency = "WPOST";
		while(rs.next()) {
			int tid = rs.getInt(1);
			
			selectTF.setInt(1, tid);
			selectTF.setString(2, agency);
			ResultSet count = selectTF.executeQuery();
			count.first();
			int idfCount = count.getInt(1);
			insertIDF.setInt(1, tid);
			insertIDF.setInt(2, idfCount);
			insertIDF.setString(3, agency);
			insertIDF.executeUpdate();
		}
		
		
	}
}
