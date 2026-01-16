package com.singleto;

/**
 * @Author: iceixQwQ
 * @Date: 2020/11/11 - 11 - 11 - 20:11
 * @Description: com.singleto
 * @Version: 1.0
 */

/**
 * 饿汉式（静态成员变量）
 * JVM保证线程安全
 */
public class Singleton01 {
   private static final Singleton01 s = new Singleton01();
   private Singleton01(){
     //最为关键
   };
   public static Singleton01 getInstance(){
       return s;
   }

   public void show(){
       System.out.println("---饿汉式成功U•ェ•*U---");
   }
}

class Test01{
    public static void main(String[] args) {
//        new  Singleton01(); //不可以
        Singleton01 s1 = Singleton01.getInstance();
        Singleton01 s2 = Singleton01.getInstance();
        if(s1 == s2){
            s1.show();
        }
    }
}