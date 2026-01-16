/*Given an array of integers, sort them in increasing order  
O(n^2)
*/
import java.util.Arrays;

public class Insertion {

    public static void main(String[] args){

        int[] a = {5, 8, 1, 3, 9 ,6};

        insertionSort(a);
    }


    public static int[] insertionSort(int[] list){
        

        for(int i = 1; i < list.length; i++) {
                int j = i; //checks and move variables to be sorted
                int hold = list[i]; //value of the current cell
            
                while (j > 0 && list[j -1] > hold){
                    list[j] = list[j -1];
                    j--;
                }
                list[j] = hold;
            }
        
            System.out.println(Arrays.toString(list));
        
        return list;
    }

}