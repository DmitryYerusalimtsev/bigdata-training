package com.longestword.reduce;

import com.longestword.common.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongestWordReducerTest {

    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        LongestWordReducer reducer = new LongestWordReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() {
        IntWritable result = new IntWritable(4);
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(1));
        testWithValues(values, result);
    }

    @Test
    public void testReducerWithNoValues() {
        IntWritable result = new IntWritable(0);
        List<IntWritable> values = new ArrayList<>();
        testWithValues(values, result);
    }

    private void testWithValues(List<IntWritable> values, IntWritable result) {
        values.add(result);
        reduceDriver.withInput(new Text(Constants.KEY), values);
        reduceDriver.withOutput(new Text(Constants.KEY), result);
        try {
            reduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
