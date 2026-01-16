/* Zachary Elinson - CSCI 2002 - Assignment #8
Create and test a bubble sort method for possible cases
*/

package bubblesort;


public class BubbleSort {

    
    public static void main(String[] args)
    {
      long start_Milliseconds;
      long end_Milliseconds;
      long total_Run_Time;
      String ascending = "ascending";
      String descending = "descending";
      String random = "random";
      
      int[] ascending_Array = generate_Ascending_Array(10);
      print_Array(ascending_Array);
      start_Milliseconds = check_Time();
      bubble_Sort_Ascend_Efficient(ascending_Array);
      end_Milliseconds = check_Time();
      total_Run_Time = calculate_Time(start_Milliseconds, end_Milliseconds);
      print_Array(ascending_Array);
      display_Time(total_Run_Time, ascending, ascending_Array.length);
      
      int[] descending_Array = generate_Descending_Array(10);
      print_Array(descending_Array);
      start_Milliseconds = check_Time();
      bubble_Sort_Ascend_Efficient(descending_Array);
      end_Milliseconds = check_Time();
      total_Run_Time = calculate_Time(start_Milliseconds, end_Milliseconds);
      print_Array(descending_Array);
      display_Time(total_Run_Time, descending, descending_Array.length);
      
      int[] random_Array10 = generate_Random_Array(10);
      print_Array(random_Array10);
      start_Milliseconds = check_Time();    
      bubble_Sort_Ascend_Efficient(random_Array10);
      end_Milliseconds = check_Time();
      total_Run_Time = calculate_Time(start_Milliseconds, end_Milliseconds);
      print_Array(random_Array10);
      display_Time(total_Run_Time, random, random_Array10.length);
      
      int[] random_Array_Large = generate_Random_Array(10000);
      start_Milliseconds = check_Time();    
      bubble_Sort_Ascend_Efficient(random_Array_Large);
      end_Milliseconds = check_Time();
      total_Run_Time = calculate_Time(start_Milliseconds, end_Milliseconds);
      display_Time(total_Run_Time, random, random_Array_Large.length);
      
      int[] descending_Array_Large = generate_Descending_Array(10000);
      start_Milliseconds = check_Time();
      bubble_Sort_Ascend_Efficient(descending_Array_Large);
      end_Milliseconds = check_Time();
      total_Run_Time = calculate_Time(start_Milliseconds, end_Milliseconds);
      display_Time(total_Run_Time, descending, descending_Array_Large.length);
    }

public static long check_Time()
{
    long current_Milliseconds = System.currentTimeMillis();
    return current_Milliseconds;
}

public static long calculate_Time(long start_Milliseconds, long end_Milliseconds)
{
    long total_Run_Time = end_Milliseconds - start_Milliseconds;
    return total_Run_Time;
}

public static void display_Time(long total_Run_Time, String array_Type, int array_Size)
{
    System.out.println("Sorting time (ms) for "+array_Size+" "+array_Type+" elements: "+ total_Run_Time);
    System.out.println(" ");
}


public static long convert_To_Seconds(long total_Run_Time)
{
    long converted_Run_Time = total_Run_Time % 60;
    return converted_Run_Time;
}

public static int[] generate_Ascending_Array(int size)
{
    int[] ascending_Array = new int[size];
    
    for(int i = 0; i < ascending_Array.length; i++)
    {
        ascending_Array[i] = i;
    }
   
    return ascending_Array;
}

public static int[] generate_Descending_Array(int size)
{
    int[] descending_Array = new int[size];
   
    
    for(int i = 0; i < descending_Array.length; i++)
    {
       descending_Array[i] = size - i - 1 ;
       
    }
    
    return descending_Array;
}

public static int[] generate_Random_Array(int size)
{
    int[] random_Array = new int[size];
    
    for(int i = 0; i < random_Array.length; i++)
    {
        random_Array[i] = (int)(Math.random() * size);
    }
    
    return random_Array;
}

public static void print_Array(int array[])
{
    for(int i = 0; i < array.length; i++)
    {
        if(i != array.length - 1)
        {
            System.out.print(array[i]+", ");
        }
        else
        {
            System.out.print(array[i]+".");
        }
    }
    
    System.out.println(" ");
}

public static void bubble_Sort_Ascend_Efficient(int[] array)
{
    int lastSwap = array.length - 1;
    boolean array_Sorted = true;
    int tempSwap = -1;
    
    for(int i = 1; i < array.length; i++)
    {
        for(int j = 0; j < lastSwap; j++)
        {
            if(array[j] > array[j+1])
            {
                int temp = array[j];
                array[j] = array[j+1];
                array[j+1] = temp;
                array_Sorted = false;
                tempSwap = j;
            }
        }
    }
    
    if(array_Sorted) return;
    lastSwap = tempSwap;
     
}
    
/* standard bubble sort, not optimized 
public static int[] bubbleSortAscend(int[] array)
{
    int temp;
    
    for(int i = 0; i < array.length - 1; i++)
    {
        for(int j = 1; j < array.length - i; j++)
        {
            if(array[j-1] > array[j])
            {
                temp = array[j-1];
                array[j-1] = array[j];
                array[j] = temp;
            }
        }
    }
} */

}