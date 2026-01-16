using System;
using System.Threading;

namespace ConsoleApplication1
{
    class MultiThreadingDemo
    {
        static void Print1()
        {
            Console.WriteLine("\nProcess-1 started");
            for (int i = 0; i < 20; i++)
            {
                Console.Write("1 ");
                Thread.Sleep(500);
            }
            Console.WriteLine("\nProcess-1 ended");
        }
        static void Print2()
        {
            Console.WriteLine("\nProcess-2 started");
            for (int i = 0; i < 20; i++)
            {
                Console.Write("2 ");
                Thread.Sleep(500);
            }
            Console.WriteLine("\nProcess-2 ended");
        }

        public static void Main()
        {
            ThreadStart ts1 = new ThreadStart(Print1);
            ThreadStart ts2 = new ThreadStart(Print2);

            Thread thread1 = new Thread(ts1);
            Thread thread2 = new Thread(ts2);

            thread1.Start();
            thread2.Start();

            //Print1();
            //Print2();


            Console.ReadLine();
        }
    }
}