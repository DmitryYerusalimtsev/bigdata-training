package com.accesslogs.map;

import com.accesslogs.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogParserTests {

    private LogParser parser;

    @Before
    public void setup() {
        parser = new LogParser();
    }

    @Test
    public void getBytesCorrect() {
        long bytes = parser.getBytes(TestConstants.INPUT);
        assertEquals("Bytes count must be 40028", 40028, bytes);
    }
}
