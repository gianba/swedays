package reuters.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReutersDataExtractor {

	private static final String IS_TOPIC_REGEX = "<TOPICS>.*</TOPICS>";
	private static final String TOPIC_ITEM_REGEX = "<D>(\\w*?)</D>";
	private static final String BODY_START_REGEX = ".*<BODY>(.*)";
	
	private static final String BODY_END = " Reuter";
	
	private static final Pattern isTopicPattern;
	private static final Pattern topicItemPattern;
	private static final Pattern bodyStartPattern;
		
	static{
		isTopicPattern = Pattern.compile(IS_TOPIC_REGEX);
		topicItemPattern = Pattern.compile(TOPIC_ITEM_REGEX);
		bodyStartPattern = Pattern.compile(BODY_START_REGEX);
	}

	public static List<String> getTopics(String line){
		List<String> topics = new ArrayList<String>();
		Matcher isTopicMatcher = isTopicPattern.matcher(line);
		if(isTopicMatcher.matches()){
			Matcher topicItemMatcher = topicItemPattern.matcher(line);
			while(topicItemMatcher.find()){
				topics.add(topicItemMatcher.group(1));
			}
		}			
		return topics;
	}
	
	public static boolean isBodyStart(String line){
		Matcher bodyStartMatcher = bodyStartPattern.matcher(line);
		return bodyStartMatcher.matches();
	}
	
	public static String getTextAfterBodyTag(String line){
		Matcher bodyStartMatcher = bodyStartPattern.matcher(line);
		if(bodyStartMatcher.matches()){
			return bodyStartMatcher.group(1);
		}else{
			throw new IllegalArgumentException();
		}
	}
	
	public static boolean isBodyEnd(String line){
		return BODY_END.equalsIgnoreCase(line);
	}
}
