package reuters.feature;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.junit.Test;

public class SequenceFileToFeatureVectorTest {

	@Test
	public void testDriver() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		
		Path input = new Path("splitted");
		Path output = new Path("extracted");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		SparseVectorsFromSequenceFiles driver = new SparseVectorsFromSequenceFiles();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{"-i", input.toString(), "-o", output.toString() });
		assertThat(exitCode, is(0));
		
	}

}
