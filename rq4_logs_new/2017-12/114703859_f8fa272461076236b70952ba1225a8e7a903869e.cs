using System;

namespace NumberOccurance
{
    class Program
    {
        static void Main(string[] args)
        {
            int[] myArray = new int[] { 4, 4, 4, 4, 8, 15, 15, 16, 16, 16, 16, 23, 23, 42 };
            int counter = 0;
            int current = 0;

            for (int i = 0; i < myArray.Length; i++)
            {
                current = myArray[i];
                while (i < myArray.Length && current == myArray[i])
                {
                    counter++;
                    i++;
                }
                Console.WriteLine(current + " : " + counter);
                counter = 0;
                i--;
            }
            Console.ReadLine();
        }
    }
}