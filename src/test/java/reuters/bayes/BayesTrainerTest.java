package reuters.bayes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.junit.Test;

public class BayesTrainerTest {

	@Test
	public void testDriver() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		
		Path input = new Path("extracted");
		Path output = new Path("trained");
		Path labelIndex = new Path("labelIndex");
		Path temporary = new Path("temp");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		TrainNaiveBayesJob driver = new TrainNaiveBayesJob();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{"-i", input.toString(), "-o", output.toString(), "-el", "-li", labelIndex.toString(), "--tempDir", temporary.toString(), "-ow" });
		assertThat(exitCode, is(0));
		
	}

}
