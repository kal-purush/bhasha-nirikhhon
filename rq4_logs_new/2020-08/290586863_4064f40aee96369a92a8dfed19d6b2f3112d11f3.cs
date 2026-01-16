using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lesson01_BasicConcept
{
    class Program
    {
        static void Main(string[] args)
        {
            decimal price = 4.50M;
            decimal price1 = 4.5000000M;
            string name = "Boob";


            Console.Title = "Conceptos Basicos";

            Console.ForegroundColor = ConsoleColor.Green;
            // Imprime el precio en moneda local al anteponer :C
            Console.WriteLine($"Name : {name} | price : {price1:C}");

            //Utilizando el metodo concat
            string s1 = " Hola ";
            string s2 = " Mundo ";
            string s3= " , Buenos dias";
            string s4 = System.String.Concat(s1,s2,s3);
            Console.WriteLine(s4);

            //Metodos de extension

            string s5 = System.String.Concat(s1, s2, s3);
            Console.WriteLine(s5.ToUpper()); // To upper pone todo la cadena a mayuscula

            string s6 = System.String.Concat(s1, s2, s3);
            Console.WriteLine(s6.Trim()); //Elimina espacios en la cadena

            string s7 = System.String.Concat(s1, s2, s3);
            Console.WriteLine(s7.Trim().ToUpper()); //Elimina espacios en la cadena + Pone a mayuscula

            string s8 = System.String.Concat(s1, s2, s3);
            Console.WriteLine(s8.TrimStart().TrimEnd()); //Elimina espacios eninjicio y fin



            Console.WriteLine("Enter close");
            _ = Console.ReadKey();

        }
    }
}