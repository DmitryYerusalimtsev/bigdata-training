package com.accesslogs.reduce;

import com.accesslogs.models.IpBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AccessLogsReducerTests {
    private ReduceDriver<Text, LongWritable, Text, IpBytesWritable> reduceDriver;

    @Before
    public void setUp() {
        AccessLogsReducer reducer = new AccessLogsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() {

        final Text key =new Text("ip");

        IpBytesWritable result = new IpBytesWritable(2, 4);
        List<LongWritable> values = new ArrayList<>();
        values.add(new LongWritable(3));
        values.add(new LongWritable(1));

        reduceDriver.withInput(key, values);
        reduceDriver.withOutput(key, result);

        try {
            reduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
