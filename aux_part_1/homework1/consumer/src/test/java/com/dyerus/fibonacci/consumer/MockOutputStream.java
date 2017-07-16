package com.dyerus.fibonacci.consumer;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.Assert;

import java.io.IOException;
import java.io.OutputStream;

class MockOutputStream extends OutputStream {

    private static final int FIRST_VALUE = 2;
    private static final int SECOND_VALUE = 7;

    private MockConsumer<String, Integer> mockConsumer;
    private int messagesCount = 0;

    MockOutputStream(MockConsumer<String, Integer> mockConsumer) {
        this.mockConsumer = mockConsumer;
    }

    @Override
    public void write(int b) throws IOException {
        messagesCount++;

        switch (messagesCount) {
            case 1:
                Assert.assertEquals(FIRST_VALUE, b);
                break;
            case 2:
                Assert.assertEquals(SECOND_VALUE, b);
                break;
            default:
                throw new RuntimeException("Test failed.");
        }

        if (messagesCount == 2) {
            mockConsumer.close();
        }
    }
}
