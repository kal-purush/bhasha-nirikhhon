using System;

namespace VetorClasse
{
    class Program
    {
        static void Main(string[] args)
        {
            
            Console.Write("Quantos produtos irá lançar? ");
            int n = int.Parse(Console.ReadLine());

            Produto[] vet = new Produto[n];

            for (int i = 0; i < n; i++)
            {
                Console.WriteLine("Entre com os dados do(s) produto(s): ");
                Console.Write("Nome do produto: ");
                string nome = Console.ReadLine();
                Console.Write("Preço do produto: ");
                double preco = double.Parse(Console.ReadLine());

                vet[i] = new Produto { Nome = nome, Preco = preco };
            }

            double sum = 0;
            for (int i = 0; i < n; i++)
            {
                sum += vet[i].Preco;
            }

            double media = sum / n;
            Console.WriteLine("A média dos preços dos produtos é " + media.ToString("F2"));
        }
    }
}