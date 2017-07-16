package com.dyerus.fibonacci.consumer;

import org.junit.Assert;

import java.io.IOException;
import java.io.OutputStream;

class MockOutputStream extends OutputStream {

    private static final int FIRST_VALUE = 2;
    private static final int SECOND_VALUE = 7;
    private static final int MESSAGES_COUNT = 2;

    private int messagesCount = 0;

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
        }
        Assert.assertEquals(MESSAGES_COUNT, messagesCount);
    }
}
