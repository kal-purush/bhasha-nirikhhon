using System;

namespace Senai_backend
{
    class Program
    {
        static void Main(string[] args)
        {
            //aqui vai o código
            Console.WriteLine("Digite seu nome");
            string nome = Console.ReadLine();
            Console.WriteLine("Bem vindo, "+nome);
            
            int valor1;
            int valor2;
            int soma;

            Console.WriteLine("Somando");
            Console.WriteLine("-----------------------");
            Console.Write("Digite o primeiro valor: ");
            valor1 = int.Parse(Console.ReadLine());
            Console.Write("Digite o segundo valor: ");
            valor2 = int.Parse(Console.ReadLine());
            soma = valor1 + valor2;
            Console.Write("A soma de "+valor1+" + "+valor2+" é igual a: ");
            Console.Write(soma);
        }
    }
}