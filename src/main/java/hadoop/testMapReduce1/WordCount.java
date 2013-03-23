package hadoop.testMapReduce1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new WordCount(), args);
		System.exit(r);
	}
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		// Compress Setting
		// Map Output Compress
		conf.setBoolean("mapred.conpress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec",
				GzipCodec.class, CompressionCodec.class);
		
		// Reduce Output Compress
		//conf.setBoolean("mapred.output.compress", true);
		//conf.setClass("mapred.output.compression.codec",
		//		GzipCodec.class, CompressionCodec.class);
		
		// Reducd Output Compress type (SequenceFiles only)
		// NONE, RECORD, BLOCK.
		//conf.set("mapred.output.compression.type", "BLOCK");
		
		Job job = Job.getInstance(conf, "wordcount");
		
		// Jar Class Setting
		job.setJarByClass(getClass());

		// Class Setting
		job.setMapperClass(Map.class);
		//job.setPartitionerClass(MyPartitioner.class);
		//job.setCombinerClass(Reduce.class);
		//job.setSortComparatorClass(MySortComparator.class);
		//job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setReducerClass(Reduce.class);
		
		// InputFormat Setting
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		
		// OutputFormat Setting
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// OutputFormatでも圧縮可能
		//TextOutputFormat.setCompressOutput(job, true);
		//TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		// MultipleOutputs
		//MultipleOutputs.addNamedOutput(job, "test",
		//		TextOutputFormat.class, Text.class, IntWritable.class);
		
		// Mapper Output Setting
		job.setMapOutputKeyClass(Text.class); // default:OutputKeyClass
		job.setMapOutputValueClass(IntWritable.class); // default:OutputValueClass
		
		// Reducer Task Num Setting
		job.setNumReduceTasks(5);
		
		// Reducer Output Setting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
}