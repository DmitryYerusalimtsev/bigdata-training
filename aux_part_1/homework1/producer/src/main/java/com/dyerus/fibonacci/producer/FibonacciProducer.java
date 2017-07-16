package com.dyerus.fibonacci.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FibonacciProducer {
    private final Producer<String, Integer> producer;
    private final int numberOfMessages;

    public FibonacciProducer(Producer<String, Integer> producer, int numberOfMessages) {
        this.producer = producer;
        this.numberOfMessages = numberOfMessages;
    }

    public void start() {
        int n = numberOfMessages;
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
            send(sum);
        }
    }

    public void stop() {
        producer.close();
    }

    private void send(int number) {
        try {
            ProducerRecord<String, Integer> record = new ProducerRecord<>(Constants.TOPIC, Integer.valueOf(number));
            producer.send(record);
        } finally {
            stop();
        }
    }
}
