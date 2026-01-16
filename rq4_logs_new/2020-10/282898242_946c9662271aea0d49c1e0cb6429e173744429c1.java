package ru.geekbrains.java_three.lesson_six;

import java.util.Arrays;

public class HomeWork {

    public static int[] getArray(int[] array) {
        int position = 0;

        for (int i = 0; i < array.length; i++) {
            if (array[i] == 4) {
                position = i + 1;
            }
        }

        if (position==0){
            System.out.println("в массиве нет 4");
        }
        int length = array.length - position;
        int[] arrayNew = new int[length];

        System.arraycopy(array, position, arrayNew, 0, length);
        return arrayNew;
    }

    public static boolean check(int[] array) {
        for (int i = 0; i <array.length ; i++) {
            if (array[i]==1 || array[i]==4){
                return true;
            }

        }
        return false;
    }


    public static void main(String[] args) {
        int[] array = {0, 4, 34, 5, 6, 7, 8, 6};
        System.out.println(Arrays.toString(getArray(array)));
        System.out.println(check(array));
    }
}