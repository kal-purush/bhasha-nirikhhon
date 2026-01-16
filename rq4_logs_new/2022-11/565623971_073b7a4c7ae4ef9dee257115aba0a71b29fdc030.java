package com.yearup.week4;

import com.google.common.base.Stopwatch;

import java.util.HashMap;
import java.util.Map;

public class Recursion {
    long recFibonacci(int n) {
        if (n <= 1) return n;
        return recFibonacci(n - 1) + recFibonacci(n - 2);
    }
   //2^10 -> 4^5 -> 4*4^4 -> 16*4^3 -> 64*4^2

   int basicPowerOf(int base, int exp, int local){
         local = local*base;
        exp--;
        if(exp>0) {
           basicPowerOf(base, exp, local);
           return local;
        }
        else return local;
   }

   int linPowerOf(int base, int exp){
       if(exp == 1)
           return base;
       else
           return base*linPowerOf(base, exp - 1);
   }
    //2^6 -> 2^3*2*3 -> 2^2*2^1*2^2*2^1
   int logPowerOf(int base, int exp){
       if(exp == 1)
           return base;
       else if(exp%2==0)
           return logPowerOf(base, exp/2)*logPowerOf(base, exp/2);
       else
           return logPowerOf(base, (exp/2))*logPowerOf(base, (exp/2)+1);
   }
    long linFibonacci(int n) {
        long low = 0;
        long high = 1;
        for (int i = 0; i < n; i++) {
            long oldHigh = high;
            high = low + high;
            low = oldHigh;
        }
        return low;
    }

    // 0, 1, 1, 2, 3, 5
    //fib(4) -> fib(3) -> fib(2) -> fib(1) -> 1
    //first = 1
    long tailFibonacci(long n, long low, long high) {
        if (n == 0)
            return low;
        if (n == 1)
            return high;
        return tailFibonacci(n - 1, high, low + high);
        //n=4, fib(4,0,1) -> fib(3, 1, 1) -> fib(2, 1, 2) -> fib(1, 2, 3) -> 3
    }
    //oldlast 0
    //last 0, 1
    //next 1, 1
    static Map<Long, Long> m = new HashMap<>();

    long memoizedFibonacci(long n) {
        if (m.get(n) == null) {
            m.put(n, memoizedFibonacci(n - 1) + memoizedFibonacci(n - 2));
        }
        return m.get(n);
    }

//    int powerOf(int n, int pow){
//        if(pow == 1)
//            return n;
//        return
//    }
    public static void main(String[] args) {
        m.put(0L, 0L);
        m.put(1L, 1L);
        Recursion fib = new Recursion();
        Stopwatch timer = Stopwatch.createUnstarted();
//figure out time
        timer.reset();
        timer.start();
        System.out.println(fib.basicPowerOf(2,5, 1));
        timer.stop();
        System.out.println("That took " + timer);

//        timer.reset();
//        timer.start();
//        System.out.println(fib.linPowerOf(2,64));
//        timer.stop();
//        System.out.println("That took " + timer);

        //fix this timer
//        timer.start();
//        System.out.println("recFibonacci: "+fib.recFibonacci(50));
//        timer.stop();
//        System.out.println("That took " + timer);
//
//        timer.reset();
//        timer.start();
//        System.out.println("linFibonacci: "+fib.linFibonacci(50));
//        timer.stop();
//        System.out.println("That took " + timer);
//        timer.reset();
//
//        timer.start();
//        System.out.println("tailFibonacci: "+fib.tailFibonacci(50, 0, 1));
//        timer.stop();
//        System.out.println("That took " + timer);
//        timer.reset();
//
//        timer.start();
//        System.out.println("memoizedFibonacci: "+fib.memoizedFibonacci(50));
//        timer.stop();
//        System.out.println("That took " + timer);


    }
}