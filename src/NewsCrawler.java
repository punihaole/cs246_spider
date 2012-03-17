import java.sql.SQLException;


public class NewsCrawler
{
	public static void main(String[] args) throws SQLException, InterruptedException
	{
		IndexDispatcher dispatch = IndexDispatcher.getInstance();
		dispatch.start();
	}
}
