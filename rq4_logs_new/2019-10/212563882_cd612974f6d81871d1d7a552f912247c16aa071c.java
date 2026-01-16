package sortowanie;

import java.util.Random;

import static sortowanie.MainSorting.*;

public class ArraysClass {

    public int [] createArray() {
        Random random = new Random();
        array1 = new int[size];
        for (int i = 0; i< array1.length; i++){
            array1[i] = random.nextInt(upperBound+1);
        }
        return array1;
    }
    public void printArray(int [] arrayToPrint) {
        for (int numb:arrayToPrint) {
            System.out.print(numb + ", ");
        }
    }
}