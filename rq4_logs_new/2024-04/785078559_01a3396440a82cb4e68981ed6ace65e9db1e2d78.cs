using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Arrays
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //var a = 56;
            //var b = "45th";
            //Console.WriteLine(a + b);
            //int[] numbers = new int[3]; //0-3
            //Console.WriteLine("Enter the number : ");
            //numbers[0] = Convert.ToInt32(Console.ReadLine());
            //Console.WriteLine("Enter the number : ");
            //numbers[1] = Convert.ToInt32(Console.ReadLine());
            //Console.WriteLine("Enter the number : ");
            //numbers[2] = Convert.ToInt32(Console.ReadLine());


            //Console.WriteLine($"{numbers[0]} {numbers[1]} {numbers[2]}");

            //traingles
            //const int anglecount = 3;
            //int[] angles = new int[anglecount];

            //for(int i = 0; i < angles.Length; i++)
            //{
            //    Console.Write($"Enter angle {i+1}: ");
            //    angles[i] =  Convert.ToInt32(Console.ReadLine());
            //}
            //for (int i = 0; i < angles.Length; i++)
            //{
            //    Console.WriteLine(angles[i]);

            //}



            int[] num = new int[]
            {
                4,2,3,1,0,6,7,8,9,5 
            };
            foreach (int i in num)
            {
                Console.Write($"{i} ");
            }
            Console.WriteLine();
            Array.Sort(num);
            foreach(int i in num)
            {
                Console.Write($"{i} ");
            }
            Console.WriteLine();
            Array.Reverse(num);
            foreach (int i in num)
            {
                Console.Write($"{i} ");
            }
            Console.WriteLine();
            //Console.WriteLine("Cleared array");
            //Array.Clear(num,5,num.Length-5);
            
            //foreach (int i in num)
            //{
            //    Console.Write($"{i} ");
            //}
            Console.WriteLine("Enret the number to search in array :");
            int search = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine($"The {search} is at position of {Array.IndexOf(num,search)}");
            Console.WriteLine($"The {search} is at position of {Array.IndexOf(num,search,5,2)}");

        }
    }
}