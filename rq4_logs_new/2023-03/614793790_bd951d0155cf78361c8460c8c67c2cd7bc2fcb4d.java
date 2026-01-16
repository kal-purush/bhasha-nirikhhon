package com.tuling.forkjoin;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.IntStream;

/**
 * @author  Fox
 */
public class Test {

    public static void main(String[] args) {


        int[] nums = {20,120,-50,300,-100};

//        int min = Integer.MAX_VALUE;
//        for (int i:nums){
//            if(i<min){
//                min = i;
//            }
//        }
        //jdk8
        //int min = IntStream.of(nums).parallel().min().getAsInt();

        //System.out.println(min);


        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        numbers.parallelStream().forEach(System.out::print);
        System.out.println("");
        numbers.parallelStream().forEachOrdered(System.out::print);

    }
}