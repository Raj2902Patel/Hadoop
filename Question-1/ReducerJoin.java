package que_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable val : value) {
			sum += val.get();
			
		}
		context.write(key, new IntWritable(sum));
	}

}
