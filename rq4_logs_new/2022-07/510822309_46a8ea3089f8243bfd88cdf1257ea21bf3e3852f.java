package ru.sber.base.syntax;
import java.util.Arrays;

public class Task1 {
    public static void main(String[] args) {
        int array[] = {5,2,6,4,2,8};
        int temp;
        System.out.println("Исходный массив: " + Arrays.toString(array));
        for (int i = 0; i < array.length-1; i++){
            for (int j = 0; j < array.length-i-1; j++){
                if (array[j] > array[j+1]){
                    temp = array[j];
                    array[j] = array[j+1];
                    array[j+1] = temp;
                }
            }
        }
        System.out.print("Получившийся массив: ");
        for (int i = 0; i < array.length; i++){
            System.out.print(array[i] + " ");
        }
    }
}