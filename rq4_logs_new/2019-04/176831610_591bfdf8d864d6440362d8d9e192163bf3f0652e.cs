using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entidades_Ejercicio_33;

namespace Ejercicio_33
{
    class Program
    {
        static void Main(string[] args)
        {
            int i;

            Console.Title = "Ejercicio_33";

            Libro nuevoLibro = new Libro();

            //Agrego páginas
            nuevoLibro[0] = "Hola";
            nuevoLibro[1] = "Mundo";

            Console.Write("Posición 1: ");
            Console.WriteLine(nuevoLibro[1]);

            //sobreescribo
            Console.WriteLine("Sobreescribo posición 1...");
            nuevoLibro[1] = "Juan";

            for (i = 0; i < 5; i++)
            {
                Console.WriteLine("{0}: {1}",i, nuevoLibro[i]);
            }
            
            Console.ReadKey();

        }
    }
}