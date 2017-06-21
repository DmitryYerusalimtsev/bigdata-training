package com.longestword;

import com.longestword.common.Constants;
import com.longestword.map.LongestWordMapper;
import com.longestword.reduce.LongestWordReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongestWordTest {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        LongestWordReducer reducer = new LongestWordReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() {
        IntWritable result = new IntWritable(6);

        mapReduceDriver.withInput(new LongWritable(), new Text(
                "first second third max"));

        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(5));
        values.add(result);
        values.add(new IntWritable(5));
        values.add(new IntWritable(3));

        mapReduceDriver.withOutput(new Text(Constants.KEY), result);

        try {
            mapReduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
