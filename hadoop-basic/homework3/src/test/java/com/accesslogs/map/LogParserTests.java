package com.accesslogs.map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogParserTests {

    private final String INPUT = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" " +
            "200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";

    private LogParser parser;

    @Before
    public void setup() {
        parser = new LogParser();
    }

    @Test
    public void getBytesCorrect() {
        long bytes = parser.getBytes(INPUT);
        assertEquals("Bytes count must be 40028", 40028, bytes);
    }
}
