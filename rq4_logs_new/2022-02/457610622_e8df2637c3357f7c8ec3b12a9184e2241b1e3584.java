package com.example.myapplication.javatest;

import android.util.Log;

/**
 * 泛型方法
 * 原则：无论何时，如果你能做到，你就该尽量使用泛型方法。也就是说，如果使用泛型方法将整个类泛型化，那么就应该使用
 * 泛型方法。另外对于一个static的方法而已，无法访问泛型类型的参数。所以如果static方法要使用泛型能力，就必须使其
 * 成为泛型方法。
 */
public class GenericityMethodTest<T> {

    public static final String TAG = GenericityMethodTest.class.toString();

    public void tryTest(){
        GenericityMethodTest<Person> test = new GenericityMethodTest();
        test.show1(new Man());
        test.show1(new Person());
//        test.show1(new Brid());

        test.show2(new Brid());
        test.show2(new Man());

        test.show3(new Brid());
        test.show3(new Man());
    }

    public void tryPrint(){
        printMsg("111",222,"asdasd",55.55);
    }

    public static class Person{

        @Override
        public String toString() {
            return "Person{}";
        }
    }

    public static class Man extends Person{
        @Override
        public String toString() {
            return "Man{}";
        }
    }

    public static class Brid{
        @Override
        public String toString() {
            return "Brid{}";
        }
    }

    public void show1(T t){
        Log.d(TAG, "show1: "+t.toString());
    }

    /**
     * 再泛型类中声明泛型方法T，此T与泛型类的不同，是一种全新的类型，不与泛型类的T相同
     * @param t
     * @param <T>
     */
    public <T> void show2(T t){
        Log.d(TAG, "show2: " + t.toString());
    }

    public <E> void show3(E e){
        Log.d(TAG, "show3: " + e.toString());
    }

    /**
     * 可变参数泛型方法
     * @param agrs
     * @param <T>
     */
    public <T> void printMsg(T... agrs){
        for (T t : agrs){
            Log.d(TAG, "printMsg: t is : " +t);
        }
    }

    /**
     * 静态方法使用泛型必须声明为泛型方法，且不能使用泛型类定义的泛型
     * @param t
     * @param <T>
     */
    public static <T> void  mustNeedMethod(T t){

    }

    public <T extends Person> T isPerson(T t){
        return t;
    }

//    public <T super Man> T isMan(T t){
//        return t;
//    }

}