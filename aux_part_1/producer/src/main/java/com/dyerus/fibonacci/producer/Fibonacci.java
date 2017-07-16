package com.dyerus.fibonacci.producer;

public class Fibonacci {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("You should specify a number of messages.");
            return;
        }

        int n = Integer.parseInt(args[0]);
        FibonacciProducer producer = new FibonacciProducer(n);
        producer.start();
    }
}
