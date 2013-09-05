package reuters.bayes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.junit.Test;

public class BayesTrainerTest {

	@Test
	public void testDriver() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
				
		
		System.out.println(new Path(Path.CUR_DIR));
		Path input = new Path("result", "training");
		Path output = new Path("result", "model");
		Path labels = new Path("result", "labels");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		TrainNaiveBayesJob driver = new TrainNaiveBayesJob();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{"-i", input.toString(), "-o", output.toString(), "-li", labels.toString(), "--tempDir", "tmp", "-ow", "-el" });
		assertThat(exitCode, is(0));
		
	}

}
