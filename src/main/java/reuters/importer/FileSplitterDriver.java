package reuters.importer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileSplitterDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(conf);
		Path out = new Path(arg0[1]);
		if(fileSystem.exists(out)){
			fileSystem.delete(out, true);
		}
		boolean storeAsSequenceFile = ! conf.getBoolean( "catOnly", false );
		storeAsSequenceFile = ! Boolean.parseBoolean( conf.get("catOnly"));
				
		Job job = new Job(conf, "file splitter");
		job.setJarByClass(FileSplitterDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(FileSplitterMapper.class);
		
		if(storeAsSequenceFile){
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new FileSplitterDriver(), args));
	}

}
