package com.dyerus.fibonacci.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class Fibonacci {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("You should specify a number of messages.");
            return;
        }

        int n = Integer.parseInt(args[0]);

        Producer<String, Integer> prod = createProducer();
        FibonacciProducer producer = new FibonacciProducer(prod, n);
        try {
            producer.start();
        } finally {
            producer.stop();
        }
    }

    private static <K, V> Producer<K, V> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox.hortonworks.com:6667");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        return new KafkaProducer<>(props);
    }
}
