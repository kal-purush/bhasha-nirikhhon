/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mergesort;

/**
 *
 * @author taha
 */
public class MergeSort {

    /**
     * @param args the command line arguments
     */
    private int[] array;
    private int[] tempMergArr;
    private int length;

    public static void main(String[] args) {
        int[] inputArr = {45, 23, 11, 89, 77, 98, 4, 28, 65, 43};
        System.out.println("Sıralanmamış dizi :");
        for (int i = 0; i < inputArr.length; i++) {
            System.out.print(inputArr[i] + "-");
        }
        MergeSort mms = new MergeSort();
        mms.sort(inputArr);
        System.out.println("Sıralanmış dizi :");
        for (int i = 0; i < inputArr.length; i++) {
            System.out.print(inputArr[i] + "-");
        }
    }

    public void sort(int inputArr[]) {
        this.array = inputArr;
        this.length = inputArr.length;
        this.tempMergArr = new int[length];
        doMergeSort(0, length - 1);
    }

    private void doMergeSort(int lowerIndex, int higherIndex) {

        if (lowerIndex < higherIndex) {
            int middle = lowerIndex + (higherIndex - lowerIndex) / 2;
            // Dizinin sol tarafını sıralar
            doMergeSort(lowerIndex, middle);
            // Dizinin sağ tarafını sıralar
            doMergeSort(middle + 1, higherIndex);
            // Her iki tarafı birleştirme 
            mergeParts(lowerIndex, middle, higherIndex);
        }
    }

    private void mergeParts(int lowerIndex, int middle, int higherIndex) {

        for (int i = lowerIndex; i <= higherIndex; i++) {
            tempMergArr[i] = array[i];
        }
        int i = lowerIndex;
        int j = middle + 1;
        int k = lowerIndex;
        while (i <= middle && j <= higherIndex) {
            if (tempMergArr[i] <= tempMergArr[j]) {
                array[k] = tempMergArr[i];
                i++;
                System.out.println("");
                System.out.println("islem yapilan eleman : " + array[k]);
            } else {
                array[k] = tempMergArr[j];
                j++;
                System.out.println("");
                System.out.println("islem yapilan eleman : " + array[k]);
            }
            k++;
        }
        while (i <= middle) {
            array[k] = tempMergArr[i];
            k++;
            i++;

        }

    }

}