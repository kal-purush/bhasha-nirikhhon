using System;
using System.Collections;
using System.Collections.Generic;

namespace Rotate2DArray
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            int[][] testArray = new int[][]
            {
                new int[] { 1, 2, 3 },
                new int[] { 4, 5, 6 },
                new int[] { 7, 8, 9 }
            };

            //int[][] testArray = new int[][]
            //{
            //    new int[] { 1, 2, 3, 4 },
            //    new int[] { 5, 6, 7, 8 },
            //    new int[] { 9, 10, 11, 12 },
            //    new int[] { 13, 14, 15, 16 }
            //};
            Console.WriteLine("Before Rotation");
            Print2DArray(testArray);
            Rotate(testArray);
            Console.WriteLine("After Rotation");
            Print2DArray(testArray);
        }

        /// <summary>
        /// This method rotates a square matrix to the right by 90 degrees.
        /// The input 2D array must be a square matrix otherwise nothing occurs.
        /// </summary>
        /// <param name="array">A square 2D array</param>
        static void Rotate(int[][] array)
        {
            Queue<int> queue = new Queue<int>();
            if (array.Length == array[0].Length)
            {
                for(int i = 0; i < array.Length; i ++)
                {
                    for(int j = array.Length - 1; j >= 0; j --)
                    {
                        queue.Enqueue(array[j][i]);
                    }
                }
                for(int i = 0; i < array.Length; i ++)
                {
                    for(int j = 0; j < array.Length; j ++)
                    {
                        array[i][j] = queue.Dequeue();
                    }
                }
            }
        }

        /// <summary>
        /// This method prints a 2D array into the console.
        /// </summary>
        /// <param name="array">The 2D array to print</param>
        static void Print2DArray(int[][] array)
        {
            for (int i = 0; i < array.Length; i++)
            {
                for (int j = 0; j < array.Length; j++)
                {
                    Console.Write($"{array[i][j]} ");
                }
                Console.WriteLine();
            }
        }
    }
}