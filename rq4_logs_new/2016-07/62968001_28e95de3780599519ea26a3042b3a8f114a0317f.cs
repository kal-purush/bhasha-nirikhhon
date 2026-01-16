using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MultipleThreadsMonteCarlo
{
    class Program
    {
        static Random r = new Random();
        const int threadCount = 8;
        static void Main(string[] args)
        {
            Thread[] tt = new Thread[threadCount];
            for(int i = 0; i<threadCount; i++)
            {
                tt[i] = new Thread(RunPiComputation);
                tt[i].Priority = ThreadPriority.BelowNormal;
                tt[i].Start();
            }
            Console.ReadLine();
        }
        static double CalculatePi(long attemptCount)
        {
            double x, y;
            long hitCount = 0;
            for (long i = 0; i < attemptCount; i++)
            {
                x = r.NextDouble();
                y = r.NextDouble();
                if (x * x + y * y < 1) ++hitCount;
                //Console.WriteLine("Hit at x={0} y={1}", x, y);
            }
            return 4.0 * hitCount / attemptCount;
        }

        static void RunPiComputation()
        {
            try
            {
                //int startTime = Environment.TickCount;
                //int endTime = 0;
                Random r = new Random(Program.r.Next() & DateTime.Now.Millisecond);
                long attemptCount = 1000000000L/threadCount;
                Console.WriteLine("RunPiComputation MTID:{0}", Thread.CurrentThread.ManagedThreadId);

                double pi = CalculatePi(attemptCount);
                Console.WriteLine("Pi = {0}, computation error = {1}", pi, Math.Abs(Math.PI - pi));

                //endTime = Environment.TickCount;
                //Console.WriteLine("Time elapsed : {0}", (endTime - startTime).ToString());
            }
            catch (ThreadAbortException)
            {
                Console.WriteLine("Thread aborted! MTID:{0}", Thread.CurrentThread.ManagedThreadId);
            }
        }
    }
}