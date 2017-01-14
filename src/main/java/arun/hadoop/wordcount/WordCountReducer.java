package arun.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    private IntWritable result = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		try
		{
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      
	      result.set(sum);
	      context.write(key, result);
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
}
