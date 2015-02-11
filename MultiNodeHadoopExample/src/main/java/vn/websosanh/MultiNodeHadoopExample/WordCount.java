package vn.websosanh.MultiNodeHadoopExample;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	public static void main(String[] args) {
		int res=0;
		try {
			res = ToolRunner.run(new Configuration(), new WordCount(),args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.exit(res);
	}


	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("WordCount");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		// We wil give 2 arguments at the run time, one in input path and other
		// is output path
		Path inp = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		// the hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.setInputPaths(conf,inp);
		FileOutputFormat.setOutputPath(conf, out);
		JobClient.runJob(conf);
		return 0;
	}
}
