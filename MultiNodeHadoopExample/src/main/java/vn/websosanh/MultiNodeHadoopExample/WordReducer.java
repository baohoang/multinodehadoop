package vn.websosanh.MultiNodeHadoopExample;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordReducer extends MapReduceBase implements
		Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	public void reduce(LongWritable arg0, Iterator<LongWritable> arg1,
			OutputCollector<LongWritable, LongWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		long sum = 0;
		while (arg1.hasNext()) {
			sum += arg1.next().get();
		}
		arg2.collect(arg0, new LongWritable(sum));
	}
}
