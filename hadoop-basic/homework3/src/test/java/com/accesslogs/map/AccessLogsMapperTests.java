package com.accesslogs.map;

import com.accesslogs.TestConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class AccessLogsMapperTests {
    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

    @Before
    public void setUp() {
        AccessLogsMapper mapper = new AccessLogsMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(), new Text(
                TestConstants.INPUT));
        String ip = TestConstants.INPUT.substring(0, TestConstants.INPUT.indexOf(" "));

        mapDriver.withOutput(new Text(ip), new LongWritable(TestConstants.BYTES_COUNT));

        try {
            mapDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
