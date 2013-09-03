package reuters.bayes;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;




public class BayesRunner {
	public static void main(String[] args) throws Throwable{
		ProgramDriver hadoopDriver = new ProgramDriver();
		hadoopDriver.addClass("trainnb", TrainNaiveBayesJob.class, "Train naive bayes job");
		hadoopDriver.driver(new String[]{"trainnb", "-i", "/user/cloudera/20news-train-vectors", "-el", "-o" ,"/user/cloudera/model", "-li", "/user/cloudera/labelindex" ,"-ow", "--tempDir", "/user/cloudera/tmp"});
		
		 //ToolRunner.run(new Configuration(), new TrainNaiveBayesJob(), new String[]{"-i", "/user/cloudera/20news-train-vectors", "-el", "-o" ,"/user/cloudera/model", "-li", "/user/cloudera/labelindex" ,"-ow"});
		 
	}
}
