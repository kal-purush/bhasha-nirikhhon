import java.util.Arrays;

public class Selection {

    public static void main(String[] args) {

        int[] a = {5, 8, 1, 3, 9 ,6};

        selectionSort(a);
    }


    public static int[] selectionSort(int[] array) {

        int hold = 0;

        for(int i = 0; i < array.length - 1; i++ ) {
            int minIndex = i;

           for(int j = i + 1; j < array.length; j++){
             if(array[j] < array[minIndex]){
                minIndex = j;
                //swap
                int temp = array[minIndex];
                array[minIndex] = array[i];
                array[i] = temp;
            }

           }

        }

        System.out.println(Arrays.toString(array));
        return array;
    }
}