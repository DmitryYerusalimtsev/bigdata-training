package com.dyerus.fibonacci.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public final class FibonacciConsumer {

    private final int messagesThreshold;
    private final KafkaConsumer<String, Integer> consumer;

    private static final int TIMEOUT = 10;

    public FibonacciConsumer(int messagesThreshold) {
        this.messagesThreshold = messagesThreshold;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "fibonacci");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Constants.TOPIC));
    }

    public void start() {
        int messageCount = 0;
        int sum = 0;
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
    }

    public void stop() {
        consumer.close();
    }
}
