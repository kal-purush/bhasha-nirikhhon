import java.util.Random;

/**
 * @author joe
 */
public class Client {
    
    public static final int SORT_BUBBLE_SIMPLE   = 0;
    public static final int SORT_BUBBLE_ENHANCED = 1;
    public static final int SORT_INSERTION       = 2;
    public static final int SORT_SELECTION       = 3;
    
    public static void printArray(int[] arr, int num) {
        //System.out.println("Array size: " + arr.length);
        for(int i = 0; i < num && i < arr.length; i++) {
            System.out.println(arr[i]);
        }
        //System.out.println("\n");
    }
    
    public static int indexOfLargestElement(int[] arr, int size) {
        int index = 0;
        for(int i = 1; i < size; i++) {
            if(arr[i] > arr[index])
                index = i;
        }
        return index;
    }
    
    public static long testSortAlgorithm(int[] arr, int sort_algorithm) {
        Random r = new Random();
        
        for(int i = 0; i < arr.length; i++) {
            arr[i] = r.nextInt();
        }
        
        printArray(arr, 0);
        
        long start_time = System.nanoTime();
        
        switch(sort_algorithm) {
            case SORT_BUBBLE_SIMPLE:
                simpleBubbleSort(arr);
                break;
            case SORT_BUBBLE_ENHANCED:
                enhancedBubbleSort(arr);
                break;
            case SORT_INSERTION:
                insertionSort(arr);
                break;
            case SORT_SELECTION:
                selectionSort(arr);
                break;
            default:
                break;
        }
        
        printArray(arr, 0);
        return System.nanoTime() - start_time;
    }
    
    public static double seconds(long nanoseconds) {
        double nano_s = (double)nanoseconds;
        return nano_s / 1000000000.0;
    }
    
    public static void main(String[] args) {
        for(int i = 10; i <= 100000; i *= 10) {
            int[] tmp_arr = new int[i];
            
            
            long t1 = testSortAlgorithm(tmp_arr, SORT_BUBBLE_SIMPLE);
            long t2 = testSortAlgorithm(tmp_arr, SORT_BUBBLE_ENHANCED);
            long t3 = testSortAlgorithm(tmp_arr, SORT_INSERTION);
            long t4 = testSortAlgorithm(tmp_arr, SORT_SELECTION);
            
            // help garbage collector
            tmp_arr = null;
        
            System.out.println("Elements: " + i);
            System.out.println("Simple Bubble Sort time:   " + seconds(t1) + " seconds");
            System.out.println("Enhanced Bubble Sort time: " + seconds(t2) + " seconds");
            System.out.println("Insertion Sort time:       " + seconds(t3) + " seconds");
            System.out.println("Selection Sort time:       " + seconds(t4) + " seconds\n");
        }
    }
    
    public static void selectionSort(int[] arr) {
        for(int i = 0; i < arr.length; i++) {
            int max = indexOfLargestElement(arr, arr.length - i);
            
            // swap the two elements
            int temp = arr[max];
            arr[max] = arr[arr.length - (i+1)];
            arr[arr.length - (i+1)] = temp;
        }
    }
    
    public static void insertionSort(int[] arr) {
        for(int i = 0; i < arr.length; i++) {
            int j = i;
            int temp = arr[i];
            
            while(j != 0 && arr[j-1] > temp) {
                arr[j] = arr[j-1];
                j--;
            }
            
            arr[j] = temp;
        }
    }
    
    public static void enhancedBubbleSort(int[] arr) {
        for(int i = 0; i < arr.length-1; i++) {
            boolean inner_swapped = false;
            for(int j = 0; j < arr.length-(1+i); j++) {
                if(arr[j] > arr[j+1]) {
                    int t = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = t;
                    inner_swapped = true;
                }
            }
            
            if(!inner_swapped)
                return;
        }
    }
    
    public static void simpleBubbleSort(int[] arr) {
        for(int i = 0; i < arr.length-1; i++) {
            for(int j = 0; j < arr.length-1; j++) {
                if(arr[j] > arr[j+1]) {
                    int t = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = t;
                }
            }
        }
    }
    
}