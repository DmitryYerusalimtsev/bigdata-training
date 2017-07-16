package com.dyerus.fibonacci.consumer;

import java.util.Scanner;

public class Fibonacci {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("You should specify a messages threshold.");
            return;
        }

        int n = Integer.parseInt(args[0]);
        FibonacciConsumer consumer = new FibonacciConsumer(n);
        consumer.start();

        Scanner in = new Scanner(System.in);

        while (true) {
            if (!in.hasNext()) continue;
            consumer.stop();
            break;
        }
    }
}
