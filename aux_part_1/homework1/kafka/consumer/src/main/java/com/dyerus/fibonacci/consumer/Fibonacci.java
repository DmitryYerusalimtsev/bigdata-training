package com.dyerus.fibonacci.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Fibonacci {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("You should specify a messages threshold.");
            return;
        }

        int n = Integer.parseInt(args[0]);

        Consumer<String, Integer> cons = createConsumer();
        FibonacciConsumer consumer = new FibonacciConsumer(cons, System.out, n);
        try {
            consumer.start();
        } finally {
            consumer.stop();
        }

        Scanner in = new Scanner(System.in);

        while (true) {
            if (!in.hasNext()) continue;
            consumer.stop();
            break;
        }
    }

    private static <K, V> Consumer<K, V> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox.hortonworks.com:6667");
        props.put("group.id", "fibonacci");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        Consumer<K, V> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(Constants.TOPIC));
        return consumer;
    }
}
