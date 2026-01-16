using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;

namespace InstanceCountControl
{
    class Program
    {
        static void Main(string[] args)
        {
            bool isFirstInstance;
            Mutex mutex = new Mutex(true, "mutex-instancecountcontrol-threading", out isFirstInstance);
            if (!isFirstInstance)
            {
                Console.WriteLine("Only one running instance of application is allowed!");
                Console.ReadLine();
                return;
            }
            Console.WriteLine("Here goes the application");
            Console.ReadLine();
        }
    }
}