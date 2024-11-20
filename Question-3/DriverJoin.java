package que_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DriverJoin {
	public static void main(String[] args) throws Exception {
			
			Path input = new Path(args[0]);
			Path output = new Path(args[1]);
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf,"que-1");
			
			
			job.setJarByClass(DriverJoin.class);
			MultipleInputs.addInputPath(job, input, TextInputFormat.class, MapperJoin.class);
			job.setReducerClass(ReducerJoin.class);
			
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, output);
			
			System.exit(job.waitForCompletion(true)?0:1);
		
		}
}
