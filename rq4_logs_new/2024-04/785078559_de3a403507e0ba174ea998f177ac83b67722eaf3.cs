using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Loops
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //----------------------------------Array-------------------------------------
            //create the array of particular size lets eg.5
            //int[] sized_arr = new int[5];

            //for (int i = 0; i < sized_arr.Length; i++)
            //{
            //    sized_arr[i] = Convert.ToInt32(Console.ReadLine());

            //}
            //Console.WriteLine();

            //for (int i = 0; i < sized_arr.Length; i++)
            //{
            //    Console.Write(sized_arr[i]);
            //    Console.Write(" ");
            //}


            //int[] arr = { 1, 3, 4, 5, 6, 7, 8, 9, 0, 7, 5, 5, 4, 5, 6, 6, };
            //Console.WriteLine(arr.Length);
            //for(int i=0;i<arr.Length; i++)
            //{
            //    Console.Write(arr[i]);
            //    Console.Write(" ");
            //}




            /*
            //string[] cars = new string[5]; 
            //string[] cars = new string[] {"toyota","Honda","ferrai","Hyundai","MG"};

            //string[] cars = { "toyota", "Honda", "ferrai", "Hyundai", "MG" };
            string[] cars = new string[5];
            //cars =  { "toyota", "Honda", "ferrai", "Hyundai", "MG" };  ------------------this give error Becaus we cant dairectaly assigne the value without NEW Keyword------------
            cars = new string[] { "toyota", "Honda", "ferrai", "Hyundai", "MG" };

            for (int i = 0; i < cars.Length; i++) {
               Console.WriteLine(cars[i]);
            }
            */

            string[] cars = { "toyota", "Honda", "ferrai", "Hyundai", "MG" };
            foreach(string i in cars) {
            
                Console.WriteLine(i);
            }
            int[] num = { 1, 3, 6, 4, 7, 8 };
            // array sort--------------
                //Array.Sort(num);
                //foreach(int i in num)
                //{
                //    Console.WriteLine(i);
                //}
                //Console.WriteLine(num.Max());
                //Console.WriteLine(num.Min());
                //Console.WriteLine(num.Sum());

            int[,] numbers = new int[2,3];
            numbers = new int[2,3] { { 1, 4, 2 }, { 3, 6, 8 } };
            Console.WriteLine(numbers.Length);
            Console.WriteLine(numbers.GetLength(0));
            Console.WriteLine(numbers.GetLength(1));
            for(int i = 0; i < numbers.GetLength(0); i++)
            {
                for(int j = 0; j < numbers.GetLength(1); j++)
                {
                    Console.WriteLine(numbers[i,j]);
                }
            }

        }
    }
}