package ru.epam.javacore.lesson_18_19_20_java_8.lessons.lesson_4;

import java.util.function.Supplier;

public class A_1_Supplier {
    private static class A {
    }

    private static class B {
    }

    public static void main(String[] args) {
        A[] aArray = new A[2];
        A[] aArrayNew = createCopy(() -> new A[aArray.length * 2], aArray);

        B[] bArray = new B[2];
        B[] bArrayNew = createCopy(() -> new B[bArray.length * 2], bArray);
    }

    private static <T> T[] createCopy(Supplier<T[]> supplier,
                                      T[] arr) {
        T[] newArray = supplier.get();
        System.arraycopy(arr, 0, newArray, 0, arr.length);
        return newArray;
    }

    private A[] increaseAndGetA(A[] arr, int newSize) {
        A[] newArray = new A[newSize];
        System.arraycopy(arr, 0, newArray, 0, arr.length);
        return newArray;
    }

    private B[] increaseAndGetB(B[] arr, int newSize) {
        B[] newArray = new B[newSize];
        System.arraycopy(arr, 0, newArray, 0, arr.length);
        return newArray;
    }

}