using System;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SortingAlgorithms
{
    internal class Program
    {
        
        //selection sort
        static int[] SelectionSort(int[] arr)
        {

            for (int i = 0; i < arr.Length; i++)
            {
                int min_index = i;
                for (int j = i + 1; j < arr.Length; j++)
                {
                    //fins min index
                    if (arr[j] < arr[min_index])
                    {
                        min_index = j;
                    }
                    
                }
                //swap element
                int temp = arr[i];
                arr[i] = arr[min_index];
                arr[min_index] = temp;
            }
            return arr;

        }
        //Bubble sort
        static int[] BubbleSort(int[] arr)
        {
            bool swaped;
            for(int i = 0; i < arr.Length-1; i++)
            {
                swaped = false;

                for(int j = 0; j < arr.Length-i-1; j++)
                {
                    if (arr[j] > arr[j + 1])
                    {
                        swaped = true;
                        int temp = arr[j];
                        arr[j] = arr[j + 1];
                        arr[j + 1] = temp;
                    }
                }
                if (swaped==false)
                {
                    break;
                }
            }
            return arr;
        } 
        static void Main(string[] args)
        {
            int[] arr = { 5, 7, 9, 4, 3, 0, 1, 2, 6, 8 };
            foreach(int i in arr)
            {
                Console.Write(i + " ");

            }
            //arr = SelectionSort(arr);
            arr = BubbleSort(arr);
            Console.WriteLine();
            foreach (int i in arr)
            {
                Console.Write(i + " ");

            }
        }
    }
}