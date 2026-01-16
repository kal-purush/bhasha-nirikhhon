using System;
using System.Threading;

// Критическая секция (critical section).

// lock - блокирует блок кода так, что в каждый отдельный момент времени, этот блок кода
// сможет использовать только один поток. Все остальные потоки ждут пока текущий поток, закончит работу.

namespace CriticalSection_01
{
    class MyClass
    {
        object block = new object();

        public void Method()
        {
            int hash = Thread.CurrentThread.GetHashCode();

            lock (block) // Закомментировать lock.
            {
                for (int counter = 0; counter < 10; counter++)
                {
                    Console.WriteLine("Поток # {0}: шаг {1}", hash, counter);
                    Thread.Sleep(100);
                }
                Console.WriteLine(new string('-', 20));                
            }
        }

        class Program
        {
            static void Main()
            {
                Console.SetWindowSize(80, 40);

                MyClass instance = new MyClass();

                for (int i = 0; i < 3; i++)
                {
                    new Thread(instance.Method).Start();
                }

                Thread.Sleep(500);

                // Delay.
                Console.ReadKey();
            }
        }
    }
}