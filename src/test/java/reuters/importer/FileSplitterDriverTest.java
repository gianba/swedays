package reuters.importer;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class FileSplitterDriverTest {
	
	@Test
	public void testDriver() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		
		Path input = new Path("Data");
		Path output = new Path("tmp", "splitted");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		FileSplitterDriver driver = new FileSplitterDriver();
		conf.setBoolean("catOnly", false);
		driver.setConf(conf);
		
		
		int exitCode = driver.run(new String[]{input.toString(), output.toString(), "-files", "topiclist" });
		assertThat(exitCode, is(0));
		
	}

}
