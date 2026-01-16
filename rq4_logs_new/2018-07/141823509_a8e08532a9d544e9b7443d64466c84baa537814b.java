package com.lambda.closure;

public class ClosureExample {
    public static void main(String args[]) {
        int a = 10;
        int b = 20;
        doProcess(a, i->System.out.println(i + b));
    }

    public static void doProcess(int i, Process p) {
        p.doProcess(i);
    }
}

interface Process {
    void doProcess(int i);
}