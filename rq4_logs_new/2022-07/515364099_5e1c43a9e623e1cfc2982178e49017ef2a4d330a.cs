using System;

namespace Multiplos
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Bem vindo ao somador de numeros multiplos");
            Console.WriteLine();

            int numero;
            int resultado = 0;

            Console.Write("Digite o numero: ");
            string? dado = Console.ReadLine();

            if (int.TryParse(dado, out numero))
            {
                if (numero <= 0)
                {
                    Console.WriteLine("O numero deve ser maior que 0");
                    Console.WriteLine("Digite enter para sair");
                    Console.ReadLine();
                    return;
                }

                for (int i = 3; i <= numero; i++)
                {

                    if (i % 3 == 0)
                    {
                        resultado += i;
                    }

                    if (i % 5 == 0)
                    {
                        resultado += i;
                    }
                }

                Console.WriteLine($"A soma dos multiplos de 3 e 5 é: {resultado}");
                Console.WriteLine("Digite enter para sair");
                Console.ReadLine();
            }
            else
            {
                Console.WriteLine("O dado informado não é um numero");
                Console.WriteLine("Digite enter para sair");
                Console.ReadLine();
                return;
            }
        }
    }
}