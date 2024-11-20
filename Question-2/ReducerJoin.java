package que_2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
		int mintemp = Integer.MAX_VALUE;
		
		for (IntWritable val: value) {
			mintemp = Math.min(mintemp, val.get());
		}
		
		context.write(key, new IntWritable(mintemp));
	}

}
