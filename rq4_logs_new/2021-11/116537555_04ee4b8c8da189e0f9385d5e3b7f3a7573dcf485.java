package com.kttest2.highfun;


import kotlin.Unit;
import kotlin.jvm.functions.Function1;

/**
 * 一个函数类型的变量是FunctionN接口的一个实现。Kotlin标准库定义了一系列的接口，
 * 这些接口对应于不同参数数量的函数：Function0<R>（没有参数的函数）、Function1<P1,R>（一个参数的函数），
 * 等等。每个接口定义了一个invoke方法，调用这个方法就会执行函数。
 * 一个函数类型的变量就是实现了对应的FunctionN接口的实现类的实例，实现类的invoke方法包含了lambda函数体
 */
public class HightFunJava {

    public static void main(String[] args) {
        HightFunTest.Companion.processTheAnswer(new Function1<Integer, Integer>() {
            @Override
            public Integer invoke(Integer integer) {
                return integer + 1;
            }
        });

        HightFunTest.Companion.processTheAnswer(number -> number + 1);

        HightFunTest.Companion.processTheJava(new Function1<Integer, Unit>() {
            @Override
            public Unit invoke(Integer integer) {
                return null;
            }
        });
    }
}