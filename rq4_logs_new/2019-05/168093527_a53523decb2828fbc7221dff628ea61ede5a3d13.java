package com.jvm.study.ch02;

public class BigAllocation {


    private static final int _1MB =1024*1024; //1M的大小
    // * 大对象直接进入老年代(Eden  2m  +1 )
    public static void main(String[] args) {
        byte[] b1,b2,b3,b4;
        b1 = new byte[1*_1MB]; //这个对象在eden区
        b2 = new byte[1*_1MB]; //这个对象在eden区
        b3 = new byte[2*_1MB];//这个对象直接进入老年代
        b4 = new byte[300*_1MB];//这个对象直接进入老年代
        /* b3 = new byte[5*_1MB];//这个对象直接进入老年代
        b3 = new byte[5*_1MB];//这个对象直接进入老年代
        b3 = new byte[5*_1MB];//这个对象直接进入老年代
        b3 = new byte[5*_1MB];//这个对象直接进入老年代
        b3 = new byte[5*_1MB];//这个对象直接进入老年代
        b3 = new byte[5*_1MB];//这个对象直接进入老年代
        b3 = new byte[5*_1MB];//这个对象直接进入老年代*/


    }
}