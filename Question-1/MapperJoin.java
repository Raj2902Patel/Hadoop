package que_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJoin extends  Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);
		
		while (token.hasMoreElements()) {
			value.set(token.nextToken());
			context.write(value, new IntWritable(1));
		}
	}
	
}
