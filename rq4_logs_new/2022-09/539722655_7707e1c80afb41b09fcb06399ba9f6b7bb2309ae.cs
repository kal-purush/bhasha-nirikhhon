using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ejercicio_3_y_4_Practico1
{
    class Program
    {
        static void Main(string[] args)
        {
            PruebaVector pv = new PruebaVector();
            pv.Cargar();
            Console.Clear();
            pv.Visualizar();
            pv.distancia();
            Console.ReadKey();
        }
    }
}