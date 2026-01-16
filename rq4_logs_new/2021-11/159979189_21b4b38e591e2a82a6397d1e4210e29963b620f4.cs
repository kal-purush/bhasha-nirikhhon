using System;
using System.Threading;

namespace ThreadAsync.AutoResetEventDemo
{
    class Program
    {
        static AutoResetEvent _are = new AutoResetEvent(true);
        static void Main(string[] args)
        {
            for (int i = 0; i < 5; i++)
            {
                new Thread(Write).Start();
            }

            //ideally the current thread that is currently writing should set the signal in ARE after completing its writing, but if some other thread sets the signal in same ARE like shown below, it causes problems. This is when Mutex is handy
            
            //Thread.Sleep(3000);
            //_are.Set();

            Console.ReadKey();
        }

        public static void Write()
        {
            
            Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} Waiting...");
            
            _are.WaitOne();

            Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} Writing...");

            //Emulating writing to a common resource
            Thread.Sleep(5000);

            Console.WriteLine($"Thread {Thread.CurrentThread.ManagedThreadId} Writing Completed....");
            _are.Set();

           

        }
    }
}