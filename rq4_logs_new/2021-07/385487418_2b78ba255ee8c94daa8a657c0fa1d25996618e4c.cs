using System;

namespace Excercise6
{
    class Program
    {
        

        static void Main(string[] args)
        {
            const int arraySize = 10;
            
            Random rnd = new Random();
            
            int[] myArray = new int[arraySize];
            int[] Array2 = new int[arraySize];
            for (int i = 0; i < arraySize; i++) myArray[i] = rnd.Next(arraySize);
            Array.Copy(myArray, Array2, arraySize);
            myArray[arraySize - 1] = -7;
            Console.Write($"Array1 is: ");
            for (int i = 0; i < arraySize; i++) Console.Write($"{myArray[i]} ");
            Console.WriteLine();
            Console.Write($"Array2 is: ");
            for (int i = 0; i < arraySize; i++) Console.Write($"{Array2[i]} ");
            Console.WriteLine();

            Console.ReadKey();
        }
    }
}