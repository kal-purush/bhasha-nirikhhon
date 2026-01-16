using System;

namespace Tarea_for
{
    class Program
    {
        static void Main(string[] args)
        {
            //1.
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i + 1);
            }

            Console.WriteLine("------------------------------------------");

            //2
            for (int j = 100; j > 0; j--)
            {
                Console.WriteLine(j);
            }

            Console.WriteLine("------------------------------------------");
            
            //3
            for (int k = 0; k < 100; k++)
            {
                if (k%2==0)
                {
                    Console.WriteLine(k);
                }
            }

            Console.WriteLine("------------------------------------------");

            //4
            int suma = 0;
            for (int m = 0; m <= 100; m++)
            {
                Console.WriteLine(m);
                    suma += m;
                
            }
            Console.WriteLine("-----------  +");
            Console.WriteLine("El resultado de la suma es: " + suma);
            Console.WriteLine("");

            Console.WriteLine("------------------------------------------");

            //5
            int contador = 0;
            for (int o = 0; o < 100; o++)
            {
               
                if (o%2!=0)
                {
                    Console.WriteLine(o);
                    contador++;
                }
            }
            Console.WriteLine("El total de impares es: " + contador);
            Console.WriteLine("------------------------------------------");

            //6

            int num = 0;
            Console.WriteLine("Ingrese un número para saber si es positivo o negativo");
            num = int.Parse(Console.ReadLine());

            if (num <0)
            {
                Console.WriteLine("Es negativo");
            }
            else
            {
                Console.WriteLine("Es positivo");
            }
            Console.WriteLine("------------------------------------------");

            //7

            for (int a = 1; a < 11; a++)
            {
                int numeros = 0;
                for (int b = 1; b < 11; b++)
                {
                    numeros += b;
                    Console.WriteLine(a);
                }
            }
            Console.WriteLine("------------------------------------------");

            



            Console.ReadKey();
        }
    }
}