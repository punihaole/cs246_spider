import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;

//Creates an Index for a document
public class Indexer extends Thread
{
	private String url;
	private File temp;
	private boolean readyForMore;
	
	private String nextUrl;
	private File nextFile;
	
	private int id;
	
	private DBManager dbm;
	private Filter filter;
	
	public Indexer(int id)
	{
		readyForMore = true;
		this.id = id;
		dbm = new DBManager();
		filter = Filter.getInstance();
	}
	
	public void prepareToIndex(String url) throws IOException
	{
		synchronized (this) {
			nextUrl = url;
			nextFile = File.createTempFile("cs246", ".txt");
		}
	}
	
	public void startIndex()
	{
		synchronized (this) {
			if (readyForMore) {
				url = nextUrl;
				temp = nextFile;
				readyForMore = false;
				downloadHTML();
			} else {
				throw new RuntimeException("Indexer " + id + ": ERROR: trying to advance Indexer while still working! Please check isReady()");
			}
		}
	}
	
	private void downloadHTML()
	{
		readyForMore = false;
		try {
			URL urlObj = new URL(url);
			ReadableByteChannel rbc = Channels.newChannel(urlObj.openStream());
			FileOutputStream fos = new FileOutputStream(temp);
			fos.getChannel().transferFrom(rbc, 0, 1 << 24);
			fos.close();
			rbc.close();
			parseHTML();
		} catch (IOException e) {
			System.out.println("Indexer " + id + ": failed to retrieve: " + url);
			signalReady();
		}
	}
	
	private void parseHTML()
	{
		String html = "";
		BufferedReader in;
		
		try {
			in = new BufferedReader(new FileReader(temp));
			String line;
			while ((line = in.readLine()) != null) {
				html += line;
			}
			in.close();
		} catch (IOException e) {
			System.out.println("Unknown error occured to input file.");
			signalReady();
		}
		System.out.println("Indexer " + id + " is parsing " + url +".");
		
		String jsouped = Jsoup.parse(html).text();
		StringTokenizer tokenizer = new StringTokenizer(jsouped);
		Map<String, Integer> tf = new HashMap<String, Integer>();
		String agency = dbm.getAgency(url);
		int docId = dbm.getDocumentId(url);
		
		while(tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			token = filter.cleanup(token);
			if (filter.filter(token))
				continue;
			
			if (tf.containsKey(token)) {
				tf.put(token, tf.get(token) + 1);
			} else {
				tf.put(token, 1);
			}
		}
		
		Set<String> terms = tf.keySet();
		for (String term : terms) {
			int tid = dbm.updateTerms(term);
			dbm.updateTF(tid, tf.get(term), docId, agency);
			dbm.updateIDF(tid, agency);
		}
		
		System.out.println("Indexer " + id + " finished parsing " + url +".");
		
		signalReady();
	}
	
	private void signalReady()
	{
		url = null;
		temp.delete();
		readyForMore = true;
		synchronized (this) {
			this.notifyAll();
		}
	}
	
	public boolean isReady()
	{
		synchronized (this) {
			return readyForMore;
		}
	}
}
