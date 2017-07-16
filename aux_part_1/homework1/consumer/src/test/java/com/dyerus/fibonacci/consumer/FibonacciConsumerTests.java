package com.dyerus.fibonacci.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class FibonacciConsumerTests {
    private MockConsumer<String, Integer> mockConsumer;
    private FibonacciConsumer consumer;

    @Before
    public void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        MockOutputStream mockStream = new MockOutputStream();
        consumer = new FibonacciConsumer(mockConsumer, mockStream, 2);
    }

    @Test
    public void testConsumer() {

        //mockConsumer.assign(Collections.singletonList(new TopicPartition(Constants.TOPIC, 0)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(Constants.TOPIC, 0), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);

        mockConsumer.addRecord(new ConsumerRecord<String, Integer>(Constants.TOPIC,
                0, 0L, null, 1));
        mockConsumer.addRecord(new ConsumerRecord<String, Integer>(Constants.TOPIC, 0,
                1L, null, 1));
        mockConsumer.addRecord(new ConsumerRecord<String, Integer>(Constants.TOPIC, 0,
                2L, null, 2));
        mockConsumer.addRecord(new ConsumerRecord<String, Integer>(Constants.TOPIC, 0,
                3L, null, 3));

        consumer.start();
    }
}
