using System;

namespace Task_25._03
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter a number: ");
            int num = int.Parse(Console.ReadLine());
            Console.WriteLine("Number is Odd?: " + IsOdd(num));
            int number = int.Parse(Console.ReadLine());
            Console.WriteLine("Number is Even?: " + IsEven(number));
        }
        public static bool IsOdd(int num)
        {
            bool check = false;
            if (num % 2 != 0)
            {
                check = true;
            }
            return check;
        }
        public static bool IsEven(int number)
        {
            bool check = true;
            if (number % 2 == 0)
            {
                check |= true;
            }
            return !check;
        }
    }
}