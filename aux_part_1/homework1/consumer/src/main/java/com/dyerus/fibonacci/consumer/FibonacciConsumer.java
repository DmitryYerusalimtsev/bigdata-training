package com.dyerus.fibonacci.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Arrays;

public final class FibonacciConsumer {

    private final int messagesThreshold;
    private final Consumer<String, Integer> consumer;

    private static final int TIMEOUT = 10;

    public FibonacciConsumer(Consumer<String, Integer> consumer, int messagesThreshold) {
        this.consumer = consumer;
        this.messagesThreshold = messagesThreshold;
        consumer.subscribe(Arrays.asList(Constants.TOPIC));
    }

    public void start() {
        int messageCount = 0;
        int sum = 0;

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(TIMEOUT);
                for (ConsumerRecord<String, Integer> record : records) {
                    sum += record.value().intValue();
                    messageCount++;

                    if (messageCount == messagesThreshold) {
                        System.out.printf("Sum = %d", sum);
                        messageCount = 0;
                    }
                }
            }
        } finally {
            stop();
        }
    }

    public void stop() {
        consumer.close();
    }
}
