package by.academy.homework2.task2;


public class Array {

    public int find(int[] arr) {


        int n = 0;
        int result = 0;
        for (int number : arr) {
             for (int i : arr) {
                if (number == i) {
                    n++;
                }
            }

            if (n%2 != 0) {
                result = number;
                break;
            }
        }
        return result;
    }

}