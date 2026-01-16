using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChaTGPTGAP
{
    internal class Program
    {
        static void Main(string[] args)
        {

            string[,] Persona = {
              {"Victor", "Ramos"},
              {"Carlos", "Ramirez"},
              {"Ana", "Arias"},
              {"Ruben", "Osoria"}
                                  };

             foreach (string[,] nombre in Persona)
            {
                Console.WriteLine("Mi nombre es : " + nombre[0, 0] + " y mi apellido: " + nombre[0, 1]);
            }

            Console.ReadKey();


            

        }
    }
}