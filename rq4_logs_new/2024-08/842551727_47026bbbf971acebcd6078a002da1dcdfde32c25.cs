using System;

namespace Excercise6
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 1; i <= 110; i++)
            {
                if (i % 3 == 0 && i % 5 == 0)
                {
                    Console.Write("CozaLoza ");
                }
                else if (i % 3 == 0 && i % 7 == 0)
                {
                    Console.Write("CozaWoza ");
                }
                else if (i % 5 == 0 && i % 7 == 0)
                {
                    Console.Write("LozaWoza ");
                }
                else if (i % 3 == 0)
                {
                    Console.Write("Coza ");
                }
                else if (i % 5 == 0)
                {
                    Console.Write("Loza ");
                }
                else if (i % 7 == 0)
                {
                    Console.Write("Woza ");
                }
                else
                {
                    Console.Write(i + " ");
                }

                if (i % 11 == 0)
                {
                    Console.WriteLine();
                }
            }

        }
    }
}