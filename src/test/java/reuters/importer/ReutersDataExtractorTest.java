package reuters.importer;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

public class ReutersDataExtractorTest {

	private static final String BODY_LINE = "<DATELINE>    SALVADOR, Feb 26 - </DATELINE><BODY>Showers continued throughout the week in";
	private static final String BODY_END = " Reuter";
	private static final String TWO_TOPIC_LINE = "<TOPICS><D>cocoa</D><D>earn</D></TOPICS>";
	
	@Test
	public void extractTopic() {
		List<String> topics = ReutersDataExtractor.getTopics(TWO_TOPIC_LINE);
		assertThat(topics.get(0), is("cocoa"));
		assertThat(topics.get(1), is("earn"));
	}

	@Test
	public void isBodyLine() {
		assertTrue(ReutersDataExtractor.isBodyStart(BODY_LINE));
	}
	
	@Test
	public void extractBodyRemainder(){
		String textAfterBodyTag = ReutersDataExtractor.getTextAfterBodyTag(BODY_LINE);
		assertThat(textAfterBodyTag, is("Showers continued throughout the week in"));
	}
	
	@Test
	public void isBodyEnd(){
		assertTrue(ReutersDataExtractor.isBodyEnd(BODY_END));
	}

}
