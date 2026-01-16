/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package quicksortbestworst;

/**
 *
 * @author taha
 */
public class QuickSortBestWorst {

    /**
     * @param args the command line arguments
     */
     private static int track = 0;
    private static int swapSayisi = 0;
    public static void quickSort(int array[]) {
        doQuickSort(array, 0, array.length - 1);

        System.out.println("\nQuick sorted order: ");
        for (int element : array) {
            System.out.print(element + " ");
        }
        System.out.println();
        System.out.println("Quick sort " + track);

    }

    private static void doQuickSort(int array[], int start, int end) {
        int pivotPoint;
        int count = 0;
        count++;
        track += count;
        if (start < end) {

            pivotPoint = partition(array, start, end);

            doQuickSort(array, start, pivotPoint - 1);

            doQuickSort(array, pivotPoint + 1, end);
        }
    }

    private static int partition(int array[], int start, int end) {
        int pivotValue;
        int endOfLeftList;
        int mid;
        int count = 0;

        mid = (start + end) / 2;
        swap(array, start, mid);
        pivotValue = array[start];
        endOfLeftList = start;
        for (int scan = start + 1; scan <= end; scan++) {
            if (array[scan] < pivotValue) {
                endOfLeftList++;
                swap(array, endOfLeftList, scan);

            }
        }

        swap(array, start, endOfLeftList);

        return endOfLeftList;
    }
    
    private static void swap(int[] array, int a, int b) {
        int temp;
        
        temp = array[a];
        array[a] = array[b];
        array[b] = temp;
        swapSayisi++;
    }
    public static void main(String[] args) {
       int dizi1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int dizi2[] = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        System.out.println("quick sort best case durumu dizinin sıralı bir şekilde gelmesi durumunda ortaya çıkar");
        quickSort(dizi1);
        System.out.println("swap sayısı : "+ swapSayisi);
        System.out.println("quick sort worst case durumu dizinin büyükten küçüğe doğru sıralı bir şekilde gelmesi durumunda ortaya çıkar");
        quickSort(dizi2);
        System.out.println("swap sayısı : "+ swapSayisi);
    }

}