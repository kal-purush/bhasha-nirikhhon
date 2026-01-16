using System;

namespace AutotestLesson1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Clear();

            Console.WriteLine("\r\nEnter X:");
            int x = int.Parse(Console.ReadLine());

            Console.WriteLine("\r\nEnter Y:");
            int y = int.Parse(Console.ReadLine());

            Console.WriteLine("\r\nEnter Z:");
            int z = int.Parse(Console.ReadLine());

            var firstResult = (x + y) * (Math.Pow(z, 2) + 1);
            Console.WriteLine("\r\nFirst expression result: {0}", firstResult);

            var secondResult = (x%z - 1) * Math.Sqrt(y);
            Console.WriteLine("\r\nSecond expression result: {0}", secondResult);

            var thirdResult = ( x * y + y * z ) / Math.Pow(z, 3);
            Console.WriteLine("\r\nThird expression result: {0}", thirdResult);

            Console.WriteLine("\r\nThe end =)");
            Console.ReadLine();
        }
    }
}