using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Heap
{
    internal class Program
    {
        public class Heap
        {
            public int size = 0;
            public int[] arr = new int[100];


            public Heap()
            {
                
                arr[0] = -1;
                size = 0;
            }
            public void insert(int val)
            {
                //T.C. :- O(logn)
                size = size + 1;
                int index = size;
                arr[index] = val;

                //currect the position of value 
                while (index > 1)
                {
                    int parent = index / 2;
                    if (arr[parent] < arr[index])
                    {
                        //swap
                        int temp = arr[parent];
                        arr[parent] = arr[index];
                        arr[index] = temp;

                        index = parent;

                    }
                    else { return; }
                }

            }



            //delte the root node 
            public void Deletion()
            {
                //check the size
                if (size == 0)
                {
                    Console.WriteLine("Nothing to delet ");
                    return;
                }

                //put last elemnt into first index
                arr[1] = arr[size];
                size--;

                //take root node to its court position
                int i = 1;
                while (i<size )
                {
                    int leftIndex = 2 * i;
                    int rightIndex = 2 * i+1;

                    if(leftIndex<size && arr[i]< arr[leftIndex])
                    {
                        //swap arr[i] and arr[leftindex]
                        int temp = arr[i];
                        arr[i] = arr[leftIndex];
                        arr[leftIndex] = temp;

                    }
                    else if (rightIndex < size && arr[i] < arr[rightIndex])
                    {
                        //swap arr[i] and arr[rightindex]
                        int temp = arr[i];
                        arr[i] = arr[rightIndex];
                        arr[rightIndex] = temp;
                    }
                    else
                    {
                        return; 
                    }


                }
                


            }

            public void print()
            {
                for (int i = 0; i <= size; i++)
                {
                    Console.Write(arr[i] + " ");
                }
                Console.WriteLine();

            }


        }

       
        //T.C. :- O(Log(n))
        static void Heapify(int[] arr ,int size ,int i)
        {
            int largest = i;
            int left = i * 2;
            int right = 2 * i + 1;

            if(left <= size && arr[largest] < arr[left])
            {
                largest = left;
            }
            if (right <= size && arr[largest] < arr[right])
            {
                largest = right;
            }

            if (largest!=i){
                //swap
                int temp = arr[largest];
                arr[largest] = arr[i];
                arr[i] = temp;

                // call funtion again

            }
        }


        static void HeapSort(int[] arr, int n)
        {
            int size = n;
            
            while (size > 1)
            {
                //step 1 :- swap size--->1
                int temp = arr[size];
                arr[size] = arr[1];
                arr[1] = temp;
                size--;
                //step 2:- heapyfy

                Heapify(arr, size, 1);


            
            }

        }


        static void Main(string[] args)
        {
            Heap h = new Heap();
            h.insert(50);
            h.insert(55);
            h.insert(53);
            h.insert(52);
            h.insert(54);
            h.print();
            h.Deletion();
            h.print();

            int[] num = { -1, 54, 53, 55, 52, 50 };
            //heapify the array
            for (int i = 5 / 2; i > 0; i--)
            {
                Heapify(num, 5, i);
            }
            Console.WriteLine("Printing the array");
            for (int i = 1; i < num.Length; i++)
            {
                Console.Write(num[i] + " ");
            }

            Console.WriteLine();
            Console.WriteLine("HEAP SORT");
            //creation of heap

            int[] arr = { -1, 70, 6, 55, 45, 50 };
            int n = arr.Length - 1;
            for (int i = n/ 2; i > 0; i--)
            {
                Heapify(arr, n, i);
            }

            //heapsort
            HeapSort(arr, n);
            //printing the array
            Console.WriteLine("Printing the array");
            for (int i = 1; i < arr.Length; i++)
            {
                Console.Write(arr[i] + " ");
            }





        }

    }
    }

