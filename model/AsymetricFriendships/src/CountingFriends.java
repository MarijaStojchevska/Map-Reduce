import java.io.IOException;
import java.util.Date;


//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class CountingFriends {

	
public static class MapperFriends extends Mapper<LongWritable, Text, Text, IntWritable>{
	    private final static IntWritable one = new IntWritable(1); 
		public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException 
		   {
			 String relations = input.toString();
			 String relationPart = relations.split(",")[0];
			 String firstName = relationPart.replace("[", "").replace("\"", "").trim();		 
			 context.write(new Text(firstName), one);
		   }
    } 
public static class ReducerFriends  extends Reducer<Text,IntWritable,Text,IntWritable> {
  public void reduce(Text friendName, Iterable<IntWritable> counts,  Context context) throws IOException, InterruptedException 
		   {
			  int sum = 0;
			  for (IntWritable count : counts) {
			  sum += count.get();
		   }
	      context.write(friendName, new IntWritable(sum));
	  }
	}
public static void main(String[] args) throws Exception {
	
		    if (args.length != 2) {
			      System.err.println("Usage: Frequency <input path> <output path>");
			      System.exit(-1);
			 }
		    
		    //Configuration conf = new Configuration();
		    BasicConfigurator.configure();
		 
			// create a Hadoop job and set the main class
		    Job job = Job.getInstance();
		    job.setJarByClass(CountingFriends.class);
		    job.setJobName("CountingFriends");
	
		    // set the input and output path
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    // set the Mapper and Reducer class
		    job.setMapperClass(MapperFriends.class);
		    job.setReducerClass(ReducerFriends.class);
	
		    // specify the type of the output
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);

		    //Compute execute time
		    Date date; 
		    long startTime, endTime; // for recording starting and end time of job
		    date = new Date(); 
		    startTime = date.getTime(); // starting timer
            job.waitForCompletion(true);
            date = new Date(); 
		    endTime = date.getTime(); //end timer
		    System.out.println("Total Time (in milliseconds) = "+ (endTime-startTime));
		    System.out.println("Total Time (in seconds) = "+ (endTime-startTime)*0.001F);
		    System.out.println("Total Time (in minutes) = "+ ((endTime-startTime)*0.001F)/60);
		    
		    // run the job
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		 
	}
}

