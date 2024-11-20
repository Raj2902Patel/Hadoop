package que_2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJoin extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		
		String year = fields[0];
		int temp = Integer.parseInt(fields[1]);
		
		context.write(new Text(year), new IntWritable(temp));
	}

}
