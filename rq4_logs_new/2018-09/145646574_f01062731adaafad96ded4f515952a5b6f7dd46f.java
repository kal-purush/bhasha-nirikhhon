package com.example.pinyintest;

import java.lang.reflect.Field;
import java.util.Stack;

public class ReflectionTest {
    public static void main(String[] args) {

        ReflectionTest t = new ReflectionTest();
        try {
            t.t();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private void t() throws IllegalAccessException {
        Hobby hobby = new Hobby();
        hobby.setId(new Integer(1));
        hobby.setName("aaa");
        Class<? extends Hobby> aClass = hobby.getClass();
        Field[] fields = aClass.getDeclaredFields();
        for (Field f : fields
                ) {
            if (!f.isAccessible()) {
                f.setAccessible(true);
            }
//            Object o = f.get(this);
            f.set(f, new Integer(1));
//            System.out.println(o);
        }
        Stack<Object> objects = new Stack<>();
    }
}