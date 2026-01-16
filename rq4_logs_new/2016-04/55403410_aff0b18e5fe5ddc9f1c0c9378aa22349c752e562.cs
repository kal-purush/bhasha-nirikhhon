using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MultiThreadingExamples
{
    public class JoinExample
    {
        public static void Call()
        {
            Thread t = new Thread(Go);
            t.Start();
            t.Join();
            //t.Abort();
            Console.WriteLine("Thread t has ended!");
        }

        static void Go()
        {
            for (int i = 0; i < 1000; i++) Console.Write("y");
        }
    }
}