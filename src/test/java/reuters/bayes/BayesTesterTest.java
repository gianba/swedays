package reuters.bayes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.test.TestNaiveBayesDriver;
import org.junit.Test;

public class BayesTesterTest {

	@Test
	public void testDriver() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
				
		Path input = new Path("result", "testing");
		Path model = new Path("result", "model");
		Path labels = new Path("result", "labels");
		Path output = new Path("result", "result");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		TestNaiveBayesDriver driver = new TestNaiveBayesDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{"-i", input.toString(), "-m", model.toString(), "-l", labels.toString(), "-ow", "-o", output.toString() });
		assertThat(exitCode, is(0));
		
	}

}
