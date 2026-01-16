using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoAlmanaque
{
    class Program
    {
        static void Main(string[] args)
        {
            /*PROGRAMA QUE CALCULA EL PROMEDIO DE TRES NOTAS DE UN ALUMNO*/

            Console.WriteLine("PROGRAMACION ESTRUCTURADA O&M 0463");
            Console.WriteLine("INGENIERIA INDUSTRIAL");

            Console.WriteLine("EMELY AQUINO         16-SIIT-1-040");
            Console.WriteLine("ELVIS DE JESUS       16-SIIT-1-050");
            Console.WriteLine("FRANK MARTE          16-SIIT-1-019");

            //INTRODUCCION O DECLARACION DE LAS VARIABLES
            int nota1, nota2, nota3;
            string linea;
            Console.Write("Ingrese la primera nota");
            linea = Console.ReadLine();
            nota1 = int.Parse(linea);
            Console.Write("Ingrese la segunda nota");
            linea = Console.ReadLine();
            nota2 = int.Parse(linea);
            Console.Write("Ingrese la tercera nota");
            linea = Console.ReadLine();
            nota3 = int.Parse(linea);
            int promedio = (nota1 + nota2 + nota3) / 3;

            //CONDICIONES IF
            if (promedio > 70)
            {
                Console.Write("Aprobado");
            }
            else
            {
                if (promedio < 65)
                {
                    Console.Write("Reprobado");
                }
                else

                //IMPLEMENTACION DEL BUCLE
                {
                    for (promedio = 5; promedio <= 70; promedio++);
                    Console.Write("Aprobado por ayuda:");
                    Console.WriteLine(promedio);
                }
            }
            Console.ReadKey();

            /* Programa realizado por:
             * EMELY AQUINO           16-SIIT-1-040 SECCION 0463
             * ELVIS DE JESUS         16-SIIT-1-050 SECCION 0463
             * FRANK MARTE            16-SIIT-1-019 SECCION 0463*/
        }
    }
}