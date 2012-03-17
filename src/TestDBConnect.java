import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;


public class TestDBConnect 
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
		PreparedStatement insert;
		try {
			conn = DriverManager.getConnection(URL, p);
			insert = conn.prepareStatement("INSERT INTO Documents(url, agency) VALUES(?, ?)");
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to initialize DBManager");
		}
		
		
		DBManager dbm = new DBManager();
		ResultSet rs = dbm.executeSelect("SELECT url,agency FROM Documents");
		while(rs.next()) {
			String url = rs.getString("url");
			String agency = rs.getString("agency");
			insert.setString(1, url);
			insert.setString(2, agency);
			insert.executeUpdate();
		}
		
		
	}
}
