using DesignPatterns.OOP.Encapsulation.Vehicles;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.OOP.Encapsulation
{
    class Program
    {
        static void Main(string[] args)
        {
            var ferrari = new Car();
            
            Console.WriteLine("I'm a car.");
            Console.WriteLine(string.Format("I have {0} wheels.", ferrari.GetNumberOfWheels()));
            Console.WriteLine(string.Format("My maximum speed is {0}.", ferrari.GetMaximumSpeed()));

            /*
            
             * car._engine is not accessible in here. This is encapsulation.
             * It is not necessary to know car._engine to use it.
             
            */

            Console.ReadKey();
        }
    }
}