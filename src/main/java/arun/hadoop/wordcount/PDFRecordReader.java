package arun.hadoop.wordcount;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

// Record Reader for reading PDF file one file at a time
public class PDFRecordReader extends RecordReader<LongWritable, Text>{
	
	private PDDocument pdfDoc;
	private PDFTextStripper pdfStripper;
	private int totalPages;
	private int currentPage;
	private LongWritable key;
	private Text value;
	private InputStream in;
	
	@Override
	public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
		
		// Open InputStream from input split
	    FileSplit split = (FileSplit) input;
	    Configuration job = context.getConfiguration();
	    final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(job);
	    in = fs.open(file);
		
	    // Create PDF Doc from input stream
	    this.pdfStripper = new PDFTextStripper();
		this.pdfDoc = PDDocument.load(in);
		this.totalPages = pdfDoc.getNumberOfPages();
		if(this.totalPages>-1)
			this.currentPage = 0;
		else
			this.currentPage = -1;
	}
	
	// read next page from PDF and assign it to next (Key,Value) pair
	private boolean readNextPage() {
		try
		{
			System.out.println("Reading Page: " + this.currentPage);
			
			// If pages exist and not last page
			if(this.currentPage>-1 && this.currentPage<this.totalPages)
			{
				 pdfStripper.setStartPage(this.currentPage);
		         pdfStripper.setEndPage(this.currentPage);
		         String parsedText = pdfStripper.getText(this.pdfDoc);
		         key = new LongWritable(this.currentPage);
		         value = new Text(parsedText);
		         this.currentPage++;
		         return true;
			}
			else // no more pages exists
			{
				key = null;
				value = null;
				return false;
			}
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return false;
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		return readNextPage();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	
		return (this.currentPage/this.totalPages);
	}

	@Override
	public void close() throws IOException {
		
		this.pdfDoc.close();
		this.in.close();
	}

}
