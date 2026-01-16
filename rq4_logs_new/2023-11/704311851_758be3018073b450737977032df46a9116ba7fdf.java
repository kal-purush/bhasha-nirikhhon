import java.lang.Comparable;
import java.util.Random;

public class Algos implements Comparable {
    public static void SelectionSort(int[] list) {
        int minIndex;
        int nextSmallest;
        //loop through each index in the array
        for (int unsortedStart = 0;  unsortedStart < list.length; unsortedStart++) {
            //set initial min to the first value in the first index of the list
            minIndex = list[unsortedStart];
            //loop through all the elements of the list to compare if less than the minIndex
            for(int currentIndex = unsortedStart + 1; currentIndex < list.length; currentIndex++) {
                //if the element is smaller than current index update the minIndex to current
                //as it is the new smallest
                if (list[currentIndex] < minIndex) {
                    minIndex = currentIndex;
                }
            nextSmallest = list[minIndex];
            list[minIndex] = list[unsortedStart];
            list[unsortedStart] = nextSmallest;
            }
        }
    }

    public static int BinarySort(Comparable[] list, Comparable target) {
        int start =0;
        int end = list.length;
        int mid = 0;

        while (start <= end) {
            mid = (start + end) / 2;
            if (list[mid].compareTo(target) == 0) {
                return mid;
            }
           
            else if (target.compareTo(list[mid]) < 0) {
                end = mid - 1;
            }
            else {
                start = mid + 1;
            }
        }
        
        return -1;
    }

     public static Integer[] generateRandomArray(int length, int min, int max) {
        Integer[] randomArray = new Integer[length];
        Random random = new Random();

        for (int i = 0; i < length; i++) {
            randomArray[i] = random.nextInt(max - min + 1) + min;
        }

        return randomArray;
    }

    public static void main(String[] args){
        Integer[] TestArray = generateRandomArray(100000, 1, 100000);
        BinarySort(TestArray, 2);
    }



}