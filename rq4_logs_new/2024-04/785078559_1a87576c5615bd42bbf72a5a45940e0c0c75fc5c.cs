using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AccesModifire
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Car myCar = new Car();

        }
    }
     class Car
    {
        private string name;
        private string color;
        private int price;
        private void PrintVarInfo()
        {
            Console.WriteLine("name "+name);
            Console.WriteLine("color"+color);
            Console.WriteLine("price "+price);
        }
    }
}