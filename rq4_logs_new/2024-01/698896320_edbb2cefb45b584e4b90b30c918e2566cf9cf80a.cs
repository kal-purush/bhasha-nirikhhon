using System;

namespace VariavelBasicas // Note: actual namespace depends on the project name.
{
    internal class Program //Classe programa
    {
        static void Main(string[] args)
        {
            Console.WriteLine("## Imprimindo o nome da minha mulher ##");
            Console.WriteLine();

            Console.WriteLine("Escreva seu nome: ");
            string nome  = Console.ReadLine();
            Console.WriteLine();
            Console.WriteLine(nome + " " + "Seja bem vindo!");

            Console.ReadLine();
        }
    }
}