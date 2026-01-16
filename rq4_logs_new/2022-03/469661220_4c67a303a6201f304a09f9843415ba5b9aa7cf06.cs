using System;

namespace _8.For_Foreach_Loop_1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Write a program in C# Sharp to display n terms of natural number and their sum.
            int input = 0;
            int sum = 0;

            Console.Write("Enter number of natural terms you want: ");
            input = Convert.ToInt32(Console.ReadLine());

            Console.WriteLine("The first " + input + " natural numbers are:");

            for(int i = 1; i <= input; i++)
            {
                sum += i;
                Console.Write(i + " ");
            }
            Console.WriteLine();

            Console.WriteLine("The Sum of Natural Number upto " + input + " terms is: " + sum);

            // Wait for keyboard press before closing terminal window
            Console.ReadKey();
        }
    }
}