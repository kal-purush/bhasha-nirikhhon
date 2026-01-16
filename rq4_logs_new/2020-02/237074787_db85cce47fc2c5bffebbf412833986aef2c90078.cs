using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace ThreadHW
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Введите число:");
            int num = int.Parse(Console.ReadLine());

            Thread thread1 = new Thread(new ParameterizedThreadStart(Sum));
            thread1.Start((object)num);
            Thread thread2 = new Thread(new ParameterizedThreadStart(Fact));
            thread2.Start((object)num);

            Console.ReadKey();
        }

        static public void Fact(object num)
        {
            Thread.Sleep(5000);
            for ( int i = (int)num-1; i >=1; i--)
            {
                num = (int)num * i;
            }
            Console.WriteLine($"Факториал: {(int)num}. Поток {Thread.CurrentThread.ManagedThreadId}");
        }
        static public void Sum(object num)
        {
            Thread.Sleep(2000);
            int tmp = (int)num;
            int tmp2 = (int)num;
            while (tmp2 > 0)
            {
                tmp = tmp + --tmp2;
            }
            Console.WriteLine($"Сумма чисел: {tmp}. Поток {Thread.CurrentThread.ManagedThreadId}"); 
        }

    }
}