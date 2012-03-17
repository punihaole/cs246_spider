import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


public class IndexDispatcher
{
	private DBManager dbm;
	private static final int NUM_INDEXERS = 8;
	private List<IndexerToURLMapper> indexers;
	
	//Singleton class that dispatches the Indexers 
	private static class IndexerHolder
	{
		public static final IndexDispatcher INSTANCE = new IndexDispatcher();
	}
	
	public static IndexDispatcher getInstance()
	{
		return IndexerHolder.INSTANCE;
	}
	
	private IndexDispatcher()
	{	
		try {
			dbm = new DBManager();
			ResultSet urls = dbm.executeSelect("SELECT url FROM Documents");
			
			urls.last();
			int urlCount = urls.getRow();
			urls.first();
			
			List<String> urlList;
			indexers = new ArrayList<IndexerToURLMapper>(NUM_INDEXERS);
			int blockSize = urlCount / NUM_INDEXERS;
			for (int i = 0; i < NUM_INDEXERS - 1; i++) {
				urlList = new ArrayList<String>(blockSize + 1);
				for (int j = 0; j < blockSize; j++) {
					urlList.add(urls.getString(1));
					urls.next();
				}
				indexers.add(new IndexerToURLMapper(urlList, i+1));
			}
			// the last indexer will just take the rest of the URLS
			urlList = new ArrayList<String>(blockSize + 1);
			urlList.add(urls.getString(1));
			while (urls.next()) {
				urlList.add(urls.getString(1));
			}
			indexers.add(new IndexerToURLMapper(urlList, NUM_INDEXERS));
			
		} catch (Exception e) {
			throw new RuntimeException("IndexDispatcher failed to initialize");
		}
	}
	
	public void start() throws InterruptedException
	{
		for (IndexerToURLMapper itum : indexers) {
			itum.start();
			
		}
		
		for (IndexerToURLMapper itum : indexers) {
			itum.join();
		}
	}
	
	class IndexerToURLMapper extends Thread
	{
		private Indexer indexer;
		private List<String> urlsToIndex;
		private int currentUrl;
		
		public IndexerToURLMapper(List<String> urls, int id)
		{
			indexer = new Indexer(id);
			urlsToIndex = urls;
			currentUrl = 0;
		}
		
		public void run()
		{
			indexer.start();
			while (currentUrl < urlsToIndex.size()) {
				synchronized(indexer) {
					while (!indexer.isReady()) {
						try {
							indexer.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					//indexer is ready for input
				}
				
				try {
					indexer.prepareToIndex(urlsToIndex.get(currentUrl));
					indexer.startIndex();
				} catch (IOException e) {
					System.err.println("FAILED TO INDEX " + urlsToIndex.get(currentUrl));
					e.printStackTrace();
				}
				currentUrl++;
			}
			
			try {
				indexer.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
