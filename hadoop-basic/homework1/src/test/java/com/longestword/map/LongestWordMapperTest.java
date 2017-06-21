package com.longestword.map;

import com.longestword.common.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LongestWordMapperTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(), new Text(
                "first second third max"));
        mapDriver.withOutput(new Text(Constants.KEY), new IntWritable(6));

        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMapperWithEmptyString() {
        mapDriver.withInput(new LongWritable(), new Text(
                ""));
        mapDriver.withOutput(new Text(Constants.KEY), new IntWritable(0));
        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
