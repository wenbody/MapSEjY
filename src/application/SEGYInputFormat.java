package application;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SEGYInputFormat extends FileInputFormat<LongWritable, BytesWritable>{
	@Override
	public RecordReader<LongWritable,BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TraceRecordReader();
	}
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException{
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file: files) {
			Path path = file.getPath();
			long length = file.getLen();
			if (length != 0) {
				BlockLocation[] blkLocations;
				if (file instanceof LocatedFileStatus) {
					blkLocations = ((LocatedFileStatus) file).getBlockLocations();
				} else {
					FileSystem fs = path.getFileSystem(job.getConfiguration());
					blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				if (isSplitable(job, path)) {
					long splitSize = 42440000;
					long bytesRemaining = length-3600;
					while (((double) bytesRemaining)/splitSize > 1.1) {
						int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
						System.out.println(length-bytesRemaining);
						splits.add(makeSplit(path, length-bytesRemaining, splitSize,
								blkLocations[blkIndex].getHosts(),
		                        blkLocations[blkIndex].getCachedHosts()));
						bytesRemaining -= splitSize;
		            } 
					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
						splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
						//System.out.println(length-bytesRemaining);
					}
		        } else {
		        	splits.add(makeSplit(path, 3600, length-3600, blkLocations[0].getHosts(),
		        			blkLocations[0].getCachedHosts()));
		        }
			} else {
				splits.add(makeSplit(path, 0, length, new String[0]));
		    }
		}
	    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
	    return splits;
    }
	
	
}
