package hadoop.testMapReduce;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

	private Mapper<LongWritable, Text, Text, IntWritable>    mapper;
	private Reducer<Text, IntWritable, Text, IntWritable>    reducer;
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver;

	@Before
	public void setUp() {

		Configuration conf = new Configuration();

		mapper = new hadoop.testMapReduce1.WordCount.Map();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>(mapper);
		mapDriver.setConfiguration(conf);

		reducer = new hadoop.testMapReduce1.WordCount.Reduce();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
		reduceDriver.setConfiguration(conf);

		driver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		driver.setMapper(mapper);
		driver.setReducer(reducer);
	}


	@Test
	public void testMap() {
		mapDriver.withInput(
				new LongWritable(1),
				new Text("We must know. We will know."));
		mapDriver.withOutput(new Text("We"), new IntWritable(1));
		mapDriver.withOutput(new Text("must"), new IntWritable(1));
		mapDriver.withOutput(new Text("know."), new IntWritable(1));
		mapDriver.withOutput(new Text("We"), new IntWritable(1));
		mapDriver.withOutput(new Text("will"), new IntWritable(1));
		mapDriver.withOutput(new Text("know."), new IntWritable(1));
		mapDriver.runTest();

		//		try {
		//			List<Pair<Text, IntWritable>> result = mapDriver.run();
		//
		//			assertEquals(6, result.size());
		//			assertEquals(new Text("We"), result.get(0).getFirst());
		//			assertEquals(new IntWritable(1), result.get(0).getSecond());
		//			assertEquals(new Text("must"), result.get(1).getFirst());
		//			assertEquals(new IntWritable(1), result.get(1).getSecond());
		//			assertEquals(new Text("know."), result.get(2).getFirst());
		//			assertEquals(new IntWritable(1), result.get(2).getSecond());
		//			assertEquals(new Text("We"), result.get(3).getFirst());
		//			assertEquals(new IntWritable(1), result.get(3).getSecond());
		//			assertEquals(new Text("will"), result.get(4).getFirst());
		//			assertEquals(new IntWritable(1), result.get(4).getSecond());
		//			assertEquals(new Text("know."), result.get(5).getFirst());
		//			assertEquals(new IntWritable(1), result.get(5).getSecond());
		//			
		//		} catch (Exception e) {
		//			e.printStackTrace();
		//		}
	}

	@Test
	public void testReduce() {
		ArrayList<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("We"), values);
		reduceDriver.withOutput(new Text("We"), new IntWritable(2));
		reduceDriver.runTest();

		//		try {
		//			List<Pair<Text, IntWritable>> result = reduceDriver.run();
		//
		//			assertEquals(1, result.size());
		//			assertEquals(new Text("We"), result.get(0).getFirst());
		//			assertEquals(new IntWritable(2), result.get(0).getSecond());
		//
		//		} catch (Exception e) {
		//			e.printStackTrace();
		//		}

	}

	@Test
	public void testMapReduce() {

		driver.withInput(
				new LongWritable(1),
				new Text("We must know. We will know."));
		driver.withOutput(new Text("We"), new IntWritable(2));
		driver.withOutput(new Text("know."), new IntWritable(2));
		driver.withOutput(new Text("must"), new IntWritable(1));
		driver.withOutput(new Text("will"), new IntWritable(1));
		driver.runTest();

		//		try {
		//			List<Pair<Text, IntWritable>> result = driver.run();
		//
		//			assertEquals(4, result.size());
		//			assertEquals(new Text("We"), result.get(0).getFirst());
		//			assertEquals(new IntWritable(2), result.get(0).getSecond());
		//			assertEquals(new Text("know."), result.get(1).getFirst());
		//			assertEquals(new IntWritable(2), result.get(1).getSecond());
		//			assertEquals(new Text("must"), result.get(2).getFirst());
		//			assertEquals(new IntWritable(1), result.get(2).getSecond());
		//			assertEquals(new Text("will"), result.get(3).getFirst());
		//			assertEquals(new IntWritable(1), result.get(3).getSecond());
		//			
		//		} catch (Exception e) {
		//			e.printStackTrace();
		//		}
	}

}
