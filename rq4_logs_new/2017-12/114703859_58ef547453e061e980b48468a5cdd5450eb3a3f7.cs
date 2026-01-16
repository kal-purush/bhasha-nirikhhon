using System;

namespace Cat_Garden
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Printing desired cat gardens!\n");
            PrintGarden1();
            Console.WriteLine("");
            PrintGarden2();
            Console.ReadLine();
        }

        static void PrintGarden1()
        {
            for(int i = 0; i < 5; i++)
            {
                for(int k = 0; k < 5; k++)
                {
                    if (i % 2 == 0) Console.Write("x ");
                    else Console.Write("y ");
                }
                Console.WriteLine("");
            }
        }

        static void PrintGarden2()
        {
            for (int i = 0; i < 5; i++)
            {
                for (int k = 0; k < 5; k++)
                {
                    if ((i % 2 == 0) && (k % 2 == 0)) Console.Write("x ");
                    else if ((i % 2 == 1) && (k % 2 == 1)) Console.Write("x ");
                    else Console.Write("y ");
                }
                Console.WriteLine("");
            }
        }
    }
}