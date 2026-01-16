package ru.otus.homework;

import ru.otus.homework.annotations.After;
import ru.otus.homework.annotations.Before;
import ru.otus.homework.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public final class Runner {

    private Runner() {
    }

    public static void main(String[] args) {
        run(TestClass.class);
    }

    private static void run(Class<?> testClass) {
        System.out.println("----- Running the test from " + testClass + "\n");
        try {
            Method[] testClassMethods = testClass.getMethods();

            Constructor<?> constructor = testClass.getConstructor();

            var testCount = 0;
            var failedTestCount = 0;

            for (Method method : testClassMethods) {
                if (method.isAnnotationPresent(Test.class)) {
                    testCount++;
                    Object object = constructor.newInstance();
                    try {
                        runMethodsWithAnnotationBefore(object, testClassMethods);
                        System.out.println("   Testing method: " + method.getName());
                        method.invoke(object);
                        runMethodsWithAnnotationAfter(object, testClassMethods);
                    } catch (Exception e) {
                        failedTestCount++;
                        System.out.println("*** Test failed, method: " + method.getName() +
                                "\n*** Fail exception: " + e + "\n");
                    }
                }
            }

            System.out.println("\nTests count is " + testCount +
                    "\nCount of successful tests is " + (testCount - failedTestCount) +
                    "\nCount of failed tests is " + failedTestCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runMethodsWithAnnotationBefore(Object object, Method[] methods) throws Exception {
        System.out.println("*** Running the methods annotated Before");
        for (Method method : methods) {
            if (method.isAnnotationPresent(Before.class)) {
                method.invoke(object);
            }
        }
        System.out.println("*** Methods with annotation Before ended");
    }

    private static void runMethodsWithAnnotationAfter(Object object, Method[] methods) throws Exception {
        System.out.println("*** Running the methods annotated After");
        for (Method method : methods) {
            if (method.isAnnotationPresent(After.class)) {
                method.invoke(object);
            }
        }
        System.out.println("*** Methods with annotation After ended\n");
    }
}