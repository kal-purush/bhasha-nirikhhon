using System;
using System.Threading;

namespace Threading
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread.CurrentThread.Name = "Главный поток";
            CheckThread(Thread.CurrentThread);
            var first_thread = new Thread(FirstThreadMethod);
            
            first_thread.Name = "Фоновый поток";
            first_thread.Priority = ThreadPriority.BelowNormal;
            first_thread.IsBackground = true;
            first_thread.Start();
            CheckThread(first_thread);

            Console.WriteLine("Главный поток завершился");
            Console.ReadLine();
        }
        private static void FirstThreadMethod()
        {
            while (true)
            {
                Console.Title = DateTime.Now.ToString();
                Thread.Sleep(100);
            }
        }
        private static void CheckThread(Thread thread)
        {
            Console.WriteLine("Поток[{0}] {1} - {2} фоновый",thread.Name,thread.ManagedThreadId,thread.IsBackground ? "" : "не ");
        }
    }
}