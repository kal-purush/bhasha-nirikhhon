package com.yearup.week4.code;

import java.util.ArrayList;
import java.util.Arrays;

public class InClassCoding {

    public static void letsRecurse(){
        System.out.println("hi");
        letsRecurse();
    }

    public static void countDownRec(int n){
        int a = n-4;
        if(n<0) return;
        countDownRec(n-1);
        System.out.println(n);
    }

        public static void countDown(int n){
        while(n>=0){
            System.out.println(n);
            n--;
        }
    }

    public static void first(int a, int b){
        second(a);
        System.out.println("first");
    }

    private static void second(int a) {
        third(a);
        System.out.println("second");
    }

    private static void third(int a) {
        System.out.println("third");
    }

    public static int fact(int n){
        if(n==1) return 1;
        int ans = n*fact(n-1);
        return ans;
    }
    public static void main(String[] args) {
        System.out.println(fact(5));
//        countDownRec(5);
//        while(true){
//            System.out.println("hello");
//        }
//        InClassCoding.letsRecurse();
    }


}