package ru.otus.homework;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class App {
    public static void main(String[] args) {
        try {
            Class<Runner> clazz = Runner.class;
            Constructor<Runner> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);

            Method run = clazz.getDeclaredMethod("run", Class.class);
            run.setAccessible(true);

            Runner runner = constructor.newInstance();

            run.invoke(runner, TestClass.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}