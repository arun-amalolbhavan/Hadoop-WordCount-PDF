package arun.hadoop.wordcount;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
		try
		{
			//match words from PDF page
    	    Pattern p = Pattern.compile(
    	    				"(?<!alpha)alpha+(?!alpha)".replace("alpha", "[a-zA-Z]")
    	    			);
    	    Matcher m = p.matcher(value.toString());
    	    while (m.find()) {
    	    	String token = m.group().toLowerCase();
    	    	// words of length greater than 2
    	    	if(token.length()>2)
    	    	{
    	    		word.set(token);
    	    		context.write(word, one);
    	    	}
    	    }
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		
	}
}
