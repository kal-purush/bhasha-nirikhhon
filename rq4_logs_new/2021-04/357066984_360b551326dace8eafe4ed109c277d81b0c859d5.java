package dita.program;

import java.util.Scanner;

public class InsertionSort {
    public static void insertionSort(int[] ar, int s)
    {       

      	for(int i=0;i<s-1;i++){
          int j = i+1;
          while(j>0){
            if(ar[j]<ar[j-1]){
              int temp = ar[j];
              ar[j] = ar[j-1];
              ar[j-1] = temp;
              j--;
            }else{
            	break;
            }
          }
          printArray(ar);
        }
    }  
    
    
      
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.print("Input Jumlah Data : ");
       int s = in.nextInt();
       int[] ar = new int[s];
       for(int i=0;i<s;i++){
           System.out.print("Input data ke-"+(i+1)+" : ");
            ar[i]=in.nextInt(); 
       }
       insertionSort(ar,s);
       
                    
    }    
    private static void printArray(int[] ar) {
        System.out.print("Sort data : ");
        for(int n: ar){
        System.out.print(n+" ");
      }
        System.out.println("");
   }
}