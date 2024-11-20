package que_3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerJoin extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private Map<String, Integer> tokenCounts = new HashMap<>();
    private int totalTokens = 0;
    private int uniqueTokens = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        tokenCounts.put(key.toString(), sum);
        totalTokens += sum;
        uniqueTokens++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : tokenCounts.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
        double averageCount = (double) totalTokens / uniqueTokens;
        context.write(new Text("AverageCount"), new IntWritable((int) Math.round(averageCount)));
    }
	

}
