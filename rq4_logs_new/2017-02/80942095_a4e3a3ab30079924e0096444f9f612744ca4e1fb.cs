using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace petSimulator
{
    class Program
    {
        static void Main(string[] args)
        {
            Cow Betty = new Cow();

            Betty.setScale(5);
            Betty.setHunger(3);

            Console.WriteLine("{0}", Betty.getScale());
            Console.WriteLine("{0}", Betty.getHunger());
            Console.ReadKey();
        }
    }
}