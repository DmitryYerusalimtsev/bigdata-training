package com.dyerus.fibonacci.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.io.OutputStream;

final class FibonacciConsumer {

    private final Consumer<String, Integer> consumer;
    private final OutputStream out;
    private final int messagesThreshold;

    private static final int TIMEOUT = 10;

    FibonacciConsumer(Consumer<String, Integer> consumer,
                      OutputStream out, int messagesThreshold) {
        this.consumer = consumer;
        this.out = out;
        this.messagesThreshold = messagesThreshold;
    }

    void start() {
        int messageCount = 0;
        int sum = 0;

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(TIMEOUT);
                for (ConsumerRecord<String, Integer> record : records) {
                    sum += record.value();
                    messageCount++;

                    if (messageCount == messagesThreshold) {
                        System.out.println(sum);
                        out.write(sum);
                        messageCount = 0;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void stop() {
        consumer.close();
    }
}
