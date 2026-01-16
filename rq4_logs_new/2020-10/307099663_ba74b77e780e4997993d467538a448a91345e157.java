package com.lysy;

import com.lysy.util.Util;

public class Main {
    public static int numberOfRollers = 30;

    public static void main(String[] args) {
        long start = System.nanoTime();
        try {
            Util.initPoles();
            Util.draw();
            System.out.println("String...");
            while (Util.isNotEnd()) {
                Util.countMoves();
                Util.move();
            }
        } finally {
            Util.draw();
            System.out.println("Count of moves is equal " + String.valueOf((int) Math.pow(2, numberOfRollers) - 1));
            System.out.println("Time of execution: " + String.valueOf(System.nanoTime() - start) + " nanosec (1/1 000 000 000 sec) ");

        }
    }

}