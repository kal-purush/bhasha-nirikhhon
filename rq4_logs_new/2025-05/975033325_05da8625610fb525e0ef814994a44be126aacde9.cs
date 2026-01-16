using System;

namespace Practice2.Task11
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int num = Int32.Parse(args[0]);
            char symbol = char.Parse(args[1].ToString());
            createAnArray(num, symbol);
            static void createAnArray(int n,char symbol)
            {
                char [] array = new char[n];
                for (int i = 0; i < n; i++)
                {
                    array[i] = symbol;
                    Console.WriteLine(array[i]);
                }
            }
        }
    }
}