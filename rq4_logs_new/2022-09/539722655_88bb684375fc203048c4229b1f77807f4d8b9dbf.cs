using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ej_1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            auto auto1 = new auto();
            auto auto2 = new auto(3.2,2.6,"rojo");
            auto auto3 = new auto(3.0, 2.0, "azul");
            auto2.setThisExtras(true, "cuero");
            auto3.setThisExtras(false,"lana");

            auto1.getInfoAuto();
            auto1.getExtra();

            Console.WriteLine();
            auto2.getInfoAuto();
            auto2.getExtra();
            Console.WriteLine();
            auto3.getInfoAuto();
            auto3.getExtra();
            Console.ReadKey();
        }
    }
}