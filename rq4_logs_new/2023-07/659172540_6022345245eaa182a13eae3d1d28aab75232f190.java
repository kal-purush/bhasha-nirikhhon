package stepikCourse.Java.arrays;

import java.util.Arrays;

public class ArrayClass {
    public static void main(String[] args) {
        int array[] = {1, 2, 4, 5, 6, 7, 3, 8, 3};
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + " "); //вывели масив
        }
        //отсортировали массив по возрастанию с помошью класса Arrays
        Arrays.sort(array);
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + " ");
        }
       //поиск по индексу элемента в массиве
        int index1 = Arrays.binarySearch(array,1 );
        System.out.println("Бинарный поиск " + index1);
    }
}