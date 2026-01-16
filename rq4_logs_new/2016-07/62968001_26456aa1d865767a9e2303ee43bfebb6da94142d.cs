using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;

namespace BarrierTest
{
    class Program
    {
        const int ThreadCount = 10;
        static Barrier barrier = new Barrier(ThreadCount, (Barrier b)=> { Console.WriteLine("\nPostBarierPhase");});
        static void Main(string[] args)
        {
            ThreadStart threadAction = () =>
            {
                for (int i = 0; i < 10; i++)
                {
                    Console.Write(i);
                    barrier.SignalAndWait();
                }
            };
            Thread[] threads = new Thread[ThreadCount];
            for(int i=0; i<ThreadCount; i++)
            {
                threads[i]=new Thread(threadAction);
                threads[i].Start();
            }
            Console.ReadLine();
        }
    }
}