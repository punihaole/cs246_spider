import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class DBManager
{
	private Statement stmt;
	private Connection conn;
	
	/*	
  	private static String USER = "cs246_remote";
	private static String PASS = "cs246_project"; 
	private static String URL = "jdbc:mysql://192.168.1.100:3306/cs246";
	*/
	private static String USER = "cs246";
	private static String PASS = "cs246"; 
	private static String URL = "jdbc:mysql://localhost:3306/cs246";
	
	private static Object lockTerms = new Object();
	private static Object lockTF = new Object();
	private static Object lockDocuments = new Object();
	private static Object lockIDF = new Object();
	
	private static String selectTidQuery = "SELECT tid FROM Terms WHERE term = ?";
	private static String insertTidQuery = "INSERT INTO Terms(term) VALUE(?)";
	
	private PreparedStatement selectTid;
	private PreparedStatement insertTid;
	
	public DBManager()
	{
		Properties p = new Properties();
		p.put("user", USER);
		p.put("password", PASS);
		try {
			conn = DriverManager.getConnection(URL, p);
			stmt = conn.createStatement();
			selectTid = conn.prepareStatement(selectTidQuery);
			insertTid = conn.prepareStatement(insertTidQuery);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to initialize DBManager");
		}
		
		
		
	}
	
	public void close()
	{
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public ResultSet executeSelect(String query)
	{
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rs;
	}
	
	public void executeUpdate(String update)
	{
		try {
			stmt.executeUpdate(update);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getAgency(String url)
	{
		ResultSet rs = null;
		String agency = "UNKWN";
		synchronized(lockDocuments) {
			try {
				
				rs = stmt.executeQuery("SELECT agency FROM Documents WHERE url='" + url + "'");
				rs.first();
				agency = rs.getString(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return agency;
	}

	public int getDocumentId(String url)
	{
		ResultSet rs = null;
		int docId = -1;
		synchronized(lockDocuments) {
			try {
				
				rs = stmt.executeQuery("SELECT did FROM Documents WHERE url='" + url + "'");
				rs.first();
				docId = rs.getInt(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return docId;
	}
	
	public void updateTF(int tid, int count, int docId, String agency)
	{
		ResultSet rs = null;
		synchronized(lockTerms) { synchronized(lockTF) {
			try {
			
				rs = executeSelect("SELECT count FROM TF JOIN Documents WHERE tid = " + tid + " AND TF.did = Documents.did AND Documents.did = " + docId + " AND TF.agency = Documents.agency AND Documents.agency = '" + agency + "'");
				rs.last();
				int rowCount = rs.getRow();
				
				
				if (rowCount == 0) {
					//do an insert
					//System.out.println("Inserting TF, adding (" + tid + " , " + docId + " , " + agency + ") with count = " + count);
					String insert = "INSERT INTO TF VALUES(" +
					                tid + ", " +
					                docId + ", " + 
					                count + ", " + 
					                "'" + agency + "')";
					executeUpdate(insert);
				} else {
					//do an update
					//System.out.println("Updating TF, updating (" + tid + " , " + docId + " , " + agency + ") with count = " + count);
					String select = "SELECT COUNT " +
				                        "FROM TF " +
                                                        "WHERE tid = '" + tid + "' AND agency = '" + agency + "'";
					ResultSet rsOldCount = executeSelect(select);
					rsOldCount.first();
					int oldCount = rsOldCount.getInt(1);
					int newCount = oldCount + count;
					String update = "UPDATE TF " +
							"SET COUNT = " + newCount + " " +
							"WHERE tid = " + tid + " " +
							  "AND did = " + docId;
					executeUpdate(update);
				}
			
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}}
	}
	
	public void updateIDF(int tid, String agency)
	{
		ResultSet rs = null;
		synchronized(lockTerms) { synchronized(lockTF) { synchronized(lockIDF) {
			try {
			
				rs = executeSelect("SELECT count FROM IDF WHERE tid = " + tid + " AND agency = '" + agency + "'");
			
				rs.last();
				int rowCount = rs.getRow();
				
				if (rowCount == 0) {
					//do an insert
					//System.out.println("Inserting IDF, adding (" + tid + " , " + agency + ")");
					String insert = "INSERT INTO IDF VALUES(" +
					                tid + ", " +
					                "(SELECT COUNT(*) " +
					                 "FROM TF AS tf JOIN Documents AS d " +
					                 "ON tf.tid = " + tid + " AND tf.did = d.did AND tf.agency = d.agency " +
					                 "WHERE tf.agency = '" + agency + "'), " +
					                 "'" + agency + "')";
					executeUpdate(insert);
				} else {
					//do an update
					//System.out.println("Updating IDF, updating (" + tid + " , " + agency + ")");
					String select = "SELECT Count " +
				                        "FROM IDF " +
                                                        "WHERE tid = " + tid + " AND agency = '" + agency + "'";
					ResultSet rsOldCount = executeSelect(select);
					rsOldCount.next();
					int oldCount = rsOldCount.getInt(1);
					int newCount = oldCount++;
					String update = "UPDATE IDF " +
							"SET COUNT =  " + newCount + " " +
							"WHERE tid = " + tid + " AND agency = '" + agency + "'";
					executeUpdate(update);
				}
			
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}}}
	}

	public int updateTerms(String term)
	{
		int tid = -1;
		synchronized(lockTerms) {
			ResultSet rs;
			try {
				selectTid.setString(1, term);
				rs = selectTid.executeQuery();
			
				rs.last();
				int rowCount = rs.getRow();
				if (rowCount == 0) {
					//do an insert
					System.out.println("Inserting " + term + " into Terms table.");
					
					insertTid.setString(1, term);
					insertTid.executeUpdate();
				}
				rs = selectTid.executeQuery();
				rs.first();
				tid = rs.getInt(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return tid;
	}
}
