package application;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TraceRecordReader extends RecordReader<LongWritable, BytesWritable>{
	private FSDataInputStream inputStream = null;
	private long start;
    private long end;
    private long pos;
    private LongWritable key = new LongWritable() ;
    private BytesWritable value = new BytesWritable();
    protected Configuration conf;
    private boolean processed = false;
    
	@Override
	public void close() throws IOException {
			inputStream.close();		
	}
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ((processed == true)? 1.0f : 0.0f);
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
		this.start = fileSplit.getStart();  
        this.end = this.start + fileSplit.getLength();
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf); 
		this.inputStream = fs.open(path);  
		inputStream.seek(this.start);  
		this.pos = this.start;		
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(this.pos < this.end) {  
            key.set(this.pos);
            byte[] TraceData = new byte[4244];
            inputStream.readFully(TraceData);
            value.set(TraceData,240,4004);  
            this.pos = inputStream.getPos();  
            return true;  
        } else {  
            processed = true;  
            return false;  
        } 
	}
}
