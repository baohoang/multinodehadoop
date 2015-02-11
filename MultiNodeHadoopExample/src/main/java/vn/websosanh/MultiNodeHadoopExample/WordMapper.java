package vn.websosanh.MultiNodeHadoopExample;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, LongWritable, LongWritable> {
	private final static LongWritable one = new LongWritable(1);

	// private Text wordText = new Text();

	public void map(LongWritable arg0, Text arg1,
			OutputCollector<LongWritable, LongWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		String line = arg1.toString();
		// StringTokenizer stringTokenizer = new StringTokenizer(line);
		String s[] = line.split(" ");
		long keyLong = Long.parseLong(s[0]);
		LongWritable key = new LongWritable(keyLong);
		arg2.collect(key, one);
		// while (stringTokenizer.hasMoreTokens()) {
		// String word = stringTokenizer.nextToken();
		// wordText.set(word);
		// arg2.collect(wordText, one);
		// }
	}
}
