package main.java.ro.sci.BubbleSortHomework;

public class BubbleSortAlgorithm {//Didn't invent this magic here, just plane-old copy-paste.

    public static void BubbleSort(int[] intArray) {
        int temp = 0;
        String name = "";

        for (int i = 0; i < intArray.length; i++) {

            for (int j = 1; j < (intArray.length - 1); j++) {

                if (intArray[j - 1] > intArray[j]) {

                    temp = intArray[j - 1];
                    intArray[j - 1] = intArray[j];
                    intArray[j] = temp;
                }
            }
        }
    }
}