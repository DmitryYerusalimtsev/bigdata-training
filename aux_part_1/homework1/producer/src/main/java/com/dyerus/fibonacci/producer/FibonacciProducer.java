package com.dyerus.fibonacci.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FibonacciProducer {
    private final Properties props;
    private final int numberOfMessages;

    public FibonacciProducer(int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;

        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    }

    public void start() {
        int n = numberOfMessages;
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {

            int a = 1;
            int b = 1;
            int sum;
            for (int i = 0; i < n; i++) {
                if (i == 0 || i == 1) {
                    sum = 1;
                } else {
                    sum = a + b;
                    a = b;
                    b = sum;
                }
                send(producer, sum);
            }
        }
    }

    private void send(KafkaProducer<String, Integer> producer, int number) {
        ProducerRecord<String, Integer> record = new ProducerRecord<>(Constants.TOPIC, Integer.valueOf(number));
        producer.send(record);
    }
}
