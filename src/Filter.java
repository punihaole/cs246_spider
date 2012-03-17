import java.util.Arrays;


public class Filter
{
	private String[] blacklist = {"the", "be", "to", "of", "and", "a", "in", "that", "have", "I", 
			              "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
			              "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
			              "or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
			              "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
			              "when", "make", "can", "like", "time", "no", "just", "him", "know", "take",
			              "person", "into", "year", "your", "good", "some", "could", "them", "see", "other",
			              "than", "then", "now", "look", "only", "come", "its", "over", "think", "also",
			              "back", "after", "use", "two", "how", "our", "work", "first" ,"well", "way", 
			              "even", "new", "want", "because", "any", "these", "give", "day", "most","us"};
	
	
	//Singleton class that dispatches the Indexers 
	private static class FilterHolder
	{
		public static final Filter INSTANCE = new Filter();
	}
	
	public static Filter getInstance()
	{
		return FilterHolder.INSTANCE;
	}
	
	private Filter()
	{
		Arrays.sort(blacklist);
	}
	
	public boolean filter(String token)
	{
		if (Arrays.binarySearch(blacklist, token) < 0)
			return false; //not in blacklist
		else
			return true;
	}
	
	public String cleanup(String token)
	{
		String word = new String(token);
		word = word.toLowerCase();
		word = word.trim();
		
		String clean = "";
		
		for (int i = 0; i < word.length(); i++) {
			char c = word.charAt(i);
			if (c >= 97 && c <= 122)
				clean += c;
		}
		
		return clean;
	}
	
}
