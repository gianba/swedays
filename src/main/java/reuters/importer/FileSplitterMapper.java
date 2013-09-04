package reuters.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileSplitterMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final String DELIMITER = Character.toString ((char) 1);
	private Set<String> validTopics = new HashSet<String>();
	private List<String> topics = new ArrayList<String>();
	private String message = "";
	private boolean isInBody = false;
	private boolean printCategoriesOnly;

	@Override
	public void setup(Context context) throws IOException {
		File file = new File("topiclist");
		if( file.exists() )
		{
			FileReader fileReader = null;
			try {
				fileReader = new FileReader( file );
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			BufferedReader reader = new BufferedReader(fileReader);
			String topicLine = reader.readLine();
			while( topicLine != null ){
				String[] split = topicLine.split(DELIMITER);
				validTopics.add( split[0] );
				topicLine = reader.readLine();
			}
		}
		printCategoriesOnly = context.getConfiguration().getBoolean("catOnly", false);
	}
	
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
		String id = "";
		InputSplit inputSplit = context.getInputSplit();
		if( inputSplit instanceof FileSplit ){
			id = ((FileSplit)inputSplit).getPath().getName() + context.getCurrentKey();
		}
		Text messageText = new Text(message);
		for(String topic : topics){
			if( validTopics.isEmpty() || validTopics.contains( topic ) ){
				if( printCategoriesOnly ){
					context.write(new Text(topic), messageText);
				} else {
					context.write(new Text( "/" + topic + "/" + id ), messageText);
				}
			}
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
