package lv.acodemy.classroom;

import java.util.Arrays;

public class PlayWithArrays {
    public static void main(String[] args) {

        // Arrays

        // Declaring Array
        // Empty array with integer data type

        int[] numbers;

        // Creating an Array
        numbers = new int[10];

        // int[] numbers = new int[10];

        System.out.println(Arrays.toString(numbers));

        // Before: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        // After: [0, 2, 0, 4, 0, 6, 0, 0, 0, 10]

        numbers[1] = 2;
        numbers[3] = 4;
        numbers[5] = 6;
        numbers[7] = 8;
        numbers[9] = 10;
        System.out.println(Arrays.toString(numbers));

        // Create int array with numbers

        int[] evenNumbers = {2, 4, 6, 8, 10, 12};

        // Create String array with names

        String[] names = {"John", "Michael", "Anna", "Maria"};
        // Print all names (separately);
        System.out.println(names[0]);
        System.out.println(names[1]);
        System.out.println(names[2]);
        System.out.println(names[3]);
        System.out.println(Arrays.toString(names));

        String[] fruits = {"Banana", "Apple", "Kiwi", "Mango", "Watermelon"};
        System.out.println(fruits[0]);
        System.out.println(fruits[1]);
        System.out.println(fruits[2]);
        System.out.println(fruits[3]);
        System.out.println(fruits[4]);

    }
}