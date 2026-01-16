using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SayiOperasyonlari
{
    class Program
    {
        static void Main(string[] args)
        {
            //cm den inch e cevirme
            // m2 den sqfeet e cevirme
            //150cm olan bir ağacım 1000m2 arazimde tek başına duruyor.

            //2.54cm = 1 inch
            //1sqfoot = 0.092903 m2  10.764

            int agac = 150;
            int alan = 1000;


            Console.WriteLine((agac/2.54f) + "inch olan bir ağacım " + (alan * 10.764f) + "sqfeet arazimde tek başına duruyor");
            Console.ReadLine();
        }
    }
}