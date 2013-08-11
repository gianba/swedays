package reuters.importer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileSplitterMapper extends Mapper<LongWritable, Text, Text, Text> {

	private List<String> topics = new ArrayList<String>();
	private String message = "";
	private boolean isInBody = false;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (isInBody) {
			isInBody = handleInBodyLine(context, line);
		} else {
			isInBody = handleOutOfBodyLine(line);
		}
	}

	private boolean handleInBodyLine(Context context, String line)
			throws IOException, InterruptedException {
		if (ReutersDataExtractor.isBodyEnd(line)) {
			storeMessage(context);
			topics.clear();
			return false;
		} else {
			message += " " + line;
			return true;
		}
	}

	private void storeMessage(Context context) throws IOException, InterruptedException {
		Text messageText = new Text(message);
		for(String topic : topics){
			context.write(new Text(topic), messageText);
		}
	}

	private boolean handleOutOfBodyLine(String line) {
		if( ReutersDataExtractor.isBodyStart(line)){
			message = ReutersDataExtractor.getTextAfterBodyTag(line);
			return true;
		}else{
			List<String> extractedTopics = ReutersDataExtractor.getTopics(line);
			if( !extractedTopics.isEmpty()){
				topics = extractedTopics;
			}
			return false;
		}
	}

}
