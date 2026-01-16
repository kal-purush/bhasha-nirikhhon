package array;

import java.util.Random;

public class UseArray {
    public static void main(String[] args) {
        int limit = 5;
        int[] array = new int[limit];
        Random random = new Random();
        for (int i=0; i<array.length; i++){
            array[i]= random.nextInt(100);
        }
        for (int j=0; j<array.length; j++){
            System.out.println(array[j]);
        }

    }
}