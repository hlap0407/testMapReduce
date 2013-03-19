package hadoop.testMapReduce;

import hadoop.testMapReduce1.WordCount.Map;
import hadoop.testMapReduce1.WordCount.Reduce;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {
	
    private Mapper<LongWritable, Text, Text, IntWritable>    mapper;
    private Reducer<Text, IntWritable, Text, IntWritable>    reducer;
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        mapper = new Map();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>(mapper);
        reducer = new Reduce();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
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
    }

	@Test
	public void testReduce() {
        ArrayList<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("We"), values);
        reduceDriver.withOutput(new Text("We"), new IntWritable(2));
        reduceDriver.runTest();
	}

}
