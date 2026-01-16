package org.learning24.java8.Lambdas;

import java.util.Comparator;

public class ComparatorLambdaExample {
    public static void main(String[] args) {

        /**
         * prior Java 8
         */

        Comparator<Integer> comparator = new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
                // 0 if o1 == o2
                //  1 if o1 < o2
                // -1 if o1 > o2
            }
        };
        System.out.println("Result of comparator: "+ comparator.compare(1,1));

        /**
         * Using Java 8
         */

        Comparator<Integer> comparatorLambda = (a,b) -> a.compareTo(b);
        System.out.println("Result of comparator using Java 8: " +comparatorLambda.compare(1,1));
    }
}