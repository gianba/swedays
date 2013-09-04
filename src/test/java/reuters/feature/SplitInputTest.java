package reuters.feature;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.utils.SplitInput;
import org.junit.Test;

public class SplitInputTest {

	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
				
		Path input = new Path("tmp", "extracted/tfidf-vectors");
		Path training = new Path("result", "training");
		Path test = new Path("result", "testing");
//		FileSystem fs = FileSystem.getLocal(conf);
//		fs.delete(output, true);
		
		SplitInput driver = new SplitInput();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{"-i", input.toString(), 
											   "--trainingOutput", training.toString(), 
											   "--testOutput", test.toString(), 
											   "--randomSelectionPct", "40",
											   "--overwrite", 
											   "--sequenceFiles",
											   "-xm", "sequential" });
		assertThat(exitCode, is(0));
	}

}
