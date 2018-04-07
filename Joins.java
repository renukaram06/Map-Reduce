import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class Joins {
	
	public static class PhotoLikesMapper
	extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key,Text value,Context context
				) throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split(",");			
			Text x = new Text();					
			Text outKey = new Text(tokens[0]);
			String photoid = tokens[1];
			String likes = tokens[4];
			x.set(photoid+ "\t" + likes);						
			Text outVal = new Text("1," + x);			
			context.write(outKey, outVal);			
			
		}
	}
	
	public static class FollowMapper
	extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key,Text value,Context context
				) throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split(",");
							
			Text outKey = new Text(tokens[0]);
			Text outVal = new Text("2," + tokens[1]);
			
			context.write(outKey, outVal);
			
		}
		
	}
	
	public static class FriendMapper
	extends Mapper<Object, Text, Text, Text>{


		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			Text outKey = new Text(tokens[0]);
			Text outVal = new Text("3," + tokens[1]);
			context.write(outKey,outVal);
			
		}
	}
	
	public static class JoinReducer
	extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable <Text> values, Context context
				) throws IOException, InterruptedException {
			
			String follow = new String();
			String friend = new String();
			ArrayList<String> pageIds = new ArrayList<String>();
			Text opval = new Text();
		    for(Text val:values){
		    	String[] tokens = val.toString().split(",");
		    	if(tokens[0].equals("1")) {
		    		String pageViewId = tokens[1];
		    		pageIds.add(pageViewId);
		    	}else if (tokens[0].equals("2")) {
		    		
		    		follow = tokens[1];
		    		
		    	}else if (tokens[0].equals("3")){
		    		
		    		friend = tokens[1];
			}
		}
		    
		String val = follow + "\t" + friend;
		opval.set(val);
		for(String id: pageIds)
			context.write(new Text(id), new Text(opval));
		
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "joins");
		job.setJarByClass(Joins.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CommentLikesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FollowMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, FriendMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
