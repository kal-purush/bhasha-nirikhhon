using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {

            //Calculadora de IMC Feita em CSharp
            //Aprendendo

            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.DarkMagenta;
            Console.Clear();

            Console.Title = "Calculadora de IMC By D3m0l1d0r";
            

            string aa1;
            double peso2;
            double altura2;
            
            Console.WriteLine("Calculadora de IMC By D3m0l1d0r");

            Console.WriteLine("Seu Peso Atual =>");
            aa1 = Console.ReadLine();
            peso2 = int.Parse(aa1);

            Console.WriteLine("Sua Altura Atual =>");
            aa1 = Console.ReadLine();
            altura2 = double.Parse(aa1);

            

            double imc;
            imc = peso2 / (altura2 * altura2);
            Console.WriteLine("Seu IMC é " + imc);

            if (imc <= 24.9)
            {
                Console.WriteLine("Peso Ideal");
            }
            if (imc >= 24.9 )
            {
                Console.WriteLine("Sobrepeso");
            }
            if (imc >= 30)
            {
                Console.WriteLine("Obeso");
            }

            Console.ReadLine();

        }
    }
}