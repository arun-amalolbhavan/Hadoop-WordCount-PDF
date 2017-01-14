package arun.hadoop.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Driver(), args);
		System.out.println("Job complete!");
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			return -1;
		}
		
		Job job = Job.getInstance(getConf(),"Word Count");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(PDFInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
