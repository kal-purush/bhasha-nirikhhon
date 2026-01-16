using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Conversion
{
    class Program
    {
        static void Main(string[] args)
        {
            string Cadena;
            int Numero1 = 5, Numero2, Resultado;

            Console.WriteLine("Ingrese un número para sumarlo: ");
            Cadena = Console.ReadLine();
            Numero2 = Int32.Parse(Cadena);
            //Numero2 = Convert.ToInt32(Cadena);

            Resultado = (Numero1 + Numero2);
            Console.WriteLine("\nEl resultado es: " + Resultado);
            Console.ReadKey();
        }
    }
}