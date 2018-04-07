import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class FilterCount {
	
	//First Mapper Class to find filter count
	public static class FilterMapper
	extends Mapper<Object, Text, Text, LongWritable>{

		private final static LongWritable one = new LongWritable(1);
		private Text filter = new Text();
		
		//Mapper function 
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			//Split comma separated input and extracting only filter column 
			String v1 = value.toString();
            String[] v2 = v1.split(",");
            String v3 = v2[3];
            
            //Converting to key-value pairs 
            Scanner s = new Scanner(v3);
            while (s.hasNext()) {
                filter.set(s.next());
                context.write(filter, one);
            }
            s.close();
		}		
	}
	
	//First Reducer Class to find filter count
	public static class FilterReducer
	extends Reducer<Text,LongWritable,Text,LongWritable> {
		private LongWritable result = new LongWritable();
		
		//Reducer function to find filter count
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			
			//Sums up each instance of filter occurrence
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	//Second Mapper Class to sort filter count
	public static class SortMapper
	extends Mapper<Text, Text, LongWritable, Text>{
		private final static LongWritable filter_frequency = new LongWritable();
		
		//Mapper function
		public void map(Text key, Text value, Context context
				) throws IOException, InterruptedException {
			
			int threshold = context.getConfiguration().getInt("limit",1000); 
			if(Integer.parseInt(value.toString())>threshold){
			//swaps key-value and gets sorted by decreasing order of key using LongWritableDecreasingComparator, the value needs to be converted to LongWritable 
			filter_frequency.set(Long.parseLong(value.toString()));
			context.write(filter_frequency,key);
			}		
		}            
	}
    
	//Second Reducer Class to sort filter count
	public static class SortReducer 
	extends Reducer<LongWritable,Text,Text,LongWritable> {
		
		//Reducer function
		public void reduce(LongWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
				for(Text v : values){
					context.write(v,key);
				}
	    }			
	}   
	
	public static class CustomPartitioner extends Partitioner <LongWritable,Text>{
		@Override
		public int getPartition(LongWritable Key,Text value, int numReduceTasks){
			String filtername = value.toString();
			if(filtername.toLowerCase().charAt(0)>='a' && filtername.toLowerCase().charAt(0)<='n'){
				return 1;
			}
			return 0;
		}
	}
	
	//Driver function
	public static void main(String[] args) throws Exception {
		
		//First mapper-reducer job configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "filter_count");
		job.setJarByClass(FilterCount.class);
		job.setMapperClass(FilterMapper.class);
		job.setCombinerClass(FilterReducer.class);
		job.setReducerClass(FilterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Second mapper-reducer job configuration
		Configuration conf2 = new Configuration();
		conf2.setInt("limit",1000);
		Job job2 = Job.getInstance(conf2, "filter_sort");
		job2.setJarByClass(FilterCount.class);
		job2.setMapperClass(SortMapper.class);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class); //to sort keys by descending order
		job2.setReducerClass(SortReducer.class);	
		
		//allows to read both key and value as text, so that key-value can be swapped easily and later the type-casted (key as LongWritable and value as text)
		job2.setInputFormatClass(KeyValueTextInputFormat.class); 
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setPartitionerClass(CustomPartitioner.class);
		job2.setNumReduceTasks(2);
		
		job.waitForCompletion(true);		
		job2.waitForCompletion(true);		
	}
}