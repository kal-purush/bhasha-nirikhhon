import java.util.Arrays;

public class Selection {
     public static void main(String[] args) {
        int[] arr={1,2,-772,0,100};
        selection(arr);
        System.out.println(Arrays.toString(arr));
     }
     static void selection(int [] arr){
      
        int last=arr.length;
        for (int i = 0; i < arr.length; i++) {
           
         // get minIndex
         int min=i;
         for (int j = i; j < last; j++) {
             if(arr[j]<arr[min]){
                 min=j;
             }  
         }
         int temp=arr[min];
         arr[min]=arr[i];
         arr[i]=temp;
       
        
        }
     }
 

    /*
     *   static void selection(int [] arr){
      
        int last=arr.length;
        for (int i = 0; i < arr.length; i++) {
           
         // get minIndex
         
         int MinIndex=getMinIndex(arr,i,last);
        swap(arr,i,MinIndex);
        
        }
     }
    private static void swap(int[] arr, int i, int minIndex) {
        int temp=arr[minIndex];
        arr[minIndex]=arr[i];
        arr[i]=temp;
    }
    private static int getMinIndex(int[] arr, int i, int last) {
        int min=i;
        for (int j = i; j < last; j++) {
            if(arr[j]<arr[min]){
                min=j;
            }  
        }
    
        return min;
    }
     */
}