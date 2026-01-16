using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task2
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("Input array size: ");

            int arraySize = int.Parse(Console.ReadLine());
            Random random = new Random();
            int[] arrayOfNumbers = new int[arraySize];

            for (int i = 0; i < arrayOfNumbers.Length; i++)
            {
                arrayOfNumbers[i] = random.Next(40);
                Console.Write($"{arrayOfNumbers[i]} ");
            }

            Console.WriteLine();

            int max = Max(arrayOfNumbers);
            int min = Min(arrayOfNumbers);
            int sum = Sum(arrayOfNumbers);
            double average = Average(arrayOfNumbers);

            Console.WriteLine($"Max: {max}");
            Console.WriteLine($"Min: {min}");
            Console.WriteLine($"Sum: {sum}");
            Console.WriteLine($"Average: {average}");

            Odd(arrayOfNumbers);


            Console.ReadLine();
        }
        static int Max(int[] numbers)
        {
            int max = numbers[0];
            for (int i = 1; i < numbers.Length; i++)
            {
                if (max < numbers[i])
                {
                    max = numbers[i];
                }
            }
            return max;
        }
        static int Min(int[] numbers)
        {
            int min = numbers[0];

            for (int i = 1; i < numbers.Length; i++)
            {
                if (min > numbers[i])
                {
                    min = numbers[i];
                }
            }
            return min;
        }
        static int Sum(int[] numbers)
        {
            int sum = 0;
            for (int i = 0; i < numbers.Length; i++)
            {
                sum += numbers[i];
            }
            return sum;
        }
        static double Average(int[] numbers)
        {
            return (double)Sum(numbers) / numbers.Length;
        }
        static void Odd(int[] numbers)
        {
            for (int i = 0; i < numbers.Length; i++)
            {
                if (numbers[i] % 2 != 0)
                {
                    Console.Write($"{numbers[i]} ");
                }
            }
        }
    }
}