using System;

namespace Exercise6
{
    class Program
    {
        private static void Main(string[] args)
        {
            int[] myArray = new int[10];
            Random r = new Random();
            for (int i = 0; i < 10; i++)
            {
                myArray[i] = r.Next(1, 101);
            }
            var myArray2 = new int[10];

            Array.Copy(myArray, myArray.GetLowerBound(0), myArray2, myArray2.GetLowerBound(0), 10);
            myArray.SetValue(-7, 9);

            Console.WriteLine(String.Join(", ", myArray));
            Console.WriteLine(String.Join(", ", myArray2));
        }
    }
}