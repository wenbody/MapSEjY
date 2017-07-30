package application;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;


public class SEGYtoData {
	public static void main(String[] args) throws Exception {
		 Configuration conf = HBaseConfiguration.create();
		 Job job = Job.getInstance(conf, "SEGYtoData");
		 job.setJarByClass(SEGYtoData.class);
		 job.setInputFormatClass(SEGYInputFormat.class);
		 job.setOutputFormatClass(PhoenixOutputFormat.class);		 
		 job.setMapperClass(SEGYMapper.class);
		 job.setReducerClass(SEGYReducer.class);
		 job.setMapOutputKeyClass(BytesWritable.class);
		 job.setMapOutputValueClass(DoubleWritable.class);		 
		 job.setOutputKeyClass(NullWritable.class);		 
		 job.setOutputValueClass(SEGYWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(job, new Path(args[1]));		 
//		 System.exit(job.waitForCompletion(true) ? 0 : 1); 
//		 job.setNumReduceTasks(0);		 
		 TableMapReduceUtil.addDependencyJars(job);
		 PhoenixMapReduceUtil.setOutput(job, "DATA4", "CDP,INLINE,TIME,"+args[1]);
		 job.waitForCompletion(true);
	}
	public static class SEGYMapper extends Mapper<LongWritable, BytesWritable, BytesWritable, DoubleWritable>{
//		private final static IntWritable one = new IntWritable(1);
//		private Text word = new Text();
		private DoubleWritable segy = new DoubleWritable();
		private BytesWritable cit = new BytesWritable();
		@Override
		public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			byte[] buf = new byte[value.getLength()];
			buf=value.getBytes();
			ByteBuffer citbytes = ByteBuffer.allocate(12);
			int cdp=(int) (627 + ((key.get()-3600)/4244)/(1852 - 1189));
		    int inline=(int) (1189 + ((key.get()-3600)/4244) % (1852 - 1189));
			for (int i = 0; i < 1001; i++){				
				int sign = (buf[4 * i] >> 7) & 0x01;
			    int exp = buf[4 * i] & 0x7f;
			    exp = exp - 64;
			    int frac = ((buf[4 * i + 1] << 16) & 0x00ff0000)
					     | ((buf[4 * i + 2] << 8) & 0x0000ff00)
					     | (buf[4 * i + 3] & 0x000000ff) & 0x00ffffff;
			    if (frac == 0){
			    	segy.set(0);
			    	citbytes.put(intToBytes(cdp));
			    	citbytes.put(intToBytes(inline));
			    	citbytes.put(intToBytes(i));
//			    	segy.setCdp(CDP);
//			    	segy.setInline(INLINE);
//			    	segy.setTime(i);
			    	cit.set(citbytes.array(),0,12);
			    	context.write(cit, segy);
			    	citbytes.clear();
				    continue;
				}    
			    segy.set((1 - 2 * sign) * frac * Math.pow(2, 4 * exp - 24));
			    citbytes.put(intToBytes(cdp));
		    	citbytes.put(intToBytes(inline));
		    	citbytes.put(intToBytes(i));
//			    segy.setCdp(CDP);
//				segy.setInline(INLINE);
//				segy.setTime(i);
		    	cit.set(citbytes.array(),0,12);
//				System.out.println(cit.toString());
				context.write(cit, segy);
				citbytes.clear();
			}
		}
	}
	public static class SEGYReducer extends Reducer<BytesWritable, DoubleWritable, NullWritable, SEGYWritable> {
		private SEGYWritable segyr = new SEGYWritable();
		@Override
		protected void reduce(BytesWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			byte[] ticbytes = new byte[12];
			ticbytes=key.getBytes();
			DoubleWritable value = new DoubleWritable();
			for (DoubleWritable val : values) {
//				System.out.println(val.get());
				value=val;
//				segyr.setTime(val.getTime());
//				segyr.setInline(val.getInline());
//				segyr.setCdp(val.getCdp());				
//				System.out.println(segyr.getRecord());
			}
			segyr.setRecord(value.get());
			segyr.setCdp(bytesToInt(ticbytes,0));
			segyr.setInline(bytesToInt(ticbytes,4));
			segyr.setTime(bytesToInt(ticbytes,8));			
			context.write(NullWritable.get(), segyr);
		}
	}
	public static byte[] intToBytes( int value ) {
	    byte[] src = new byte[4];  
	    src[3] =  (byte) ((value>>24) & 0xFF);  
	    src[2] =  (byte) ((value>>16) & 0xFF);  
	    src[1] =  (byte) ((value>>8) & 0xFF);    
	    src[0] =  (byte) (value & 0xFF);                  
	    return src;   
	} 
	public static int bytesToInt(byte[] src, int offset) {  
	    int value;    
	    value = (int) ((src[offset] & 0xFF)   
	            | ((src[offset+1] & 0xFF)<<8)   
	            | ((src[offset+2] & 0xFF)<<16)   
	            | ((src[offset+3] & 0xFF)<<24));  
	    return value;  
	}  
	
}	
//	private static SEGYWritable[] toTREdata1(byte[] buf,int num,int cdp,int inline) throws SQLException{
//		SEGYWritable[] segy = new SEGYWritable[num];
//		for (int i = 0; i < num; i++){
//			int sign = (buf[4 * i] >> 7) & 0x01;
//		    int exp = buf[4 * i] & 0x7f;
//		    exp = exp - 64;
//		    int frac = ((buf[4 * i + 1] << 16) & 0x00ff0000)
//				     | ((buf[4 * i + 2] << 8) & 0x0000ff00)
//				     | (buf[4 * i + 3] & 0x000000ff) & 0x00ffffff;
//		    if (frac == 0){
//		    	segy[i].setRecord(0);
//		    	segy[i].setCdp(cdp);
//		    	segy[i].setInline(inline);
//		    	segy[i].setTime(i);
//			    continue;
//			}    
//		    segy[i].setRecord((1 - 2 * sign) * frac * Math.pow(2, 4 * exp - 24));
//		    segy[i].setCdp(cdp);
//	    	segy[i].setInline(inline);
//	    	segy[i].setTime(i);
//		}
//		return segy;
//	}


//private static SEGYWritable[] toTREdata1(byte[] buf,int num,int cdp,int inline) throws SQLException{
//	SEGYWritable[] segy = new SEGYWritable[num];
//	for (int i = 0; i < num; i++){
//		int sign = (buf[4 * i] >> 7) & 0x01;
//	    int exp = buf[4 * i] & 0x7f;
//	    exp = exp - 64;
//	    int frac = ((buf[4 * i + 1] << 16) & 0x00ff0000)
//			     | ((buf[4 * i + 2] << 8) & 0x0000ff00)
//			     | (buf[4 * i + 3] & 0x000000ff) & 0x00ffffff;
//	    if (frac == 0){
//	    	segy[i].setRecord(0);
//	    	segy[i].setCdp(cdp);
//	    	segy[i].setInline(inline);
//	    	segy[i].setTime(i);
//		    continue;
//		}    
//	    segy[i].setRecord((1 - 2 * sign) * frac * Math.pow(2, 4 * exp - 24));
//	    segy[i].setCdp(cdp);
//    	segy[i].setInline(inline);
//    	segy[i].setTime(i);
//	}
//	return segy;
//}
