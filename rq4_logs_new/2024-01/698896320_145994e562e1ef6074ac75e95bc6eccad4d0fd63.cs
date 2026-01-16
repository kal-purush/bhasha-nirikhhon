using System;
using System.Globalization;

namespace curso
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine(" ##\n Vetores ##");
            //Vetor corresponde a uma coleção de dados de tamanho fixo indexada
            //Indexada: os elementos são acessador por meio de índices
            //Unidimensional: uma dimensão
            //Homogênea: todos os dados são do mesmo tipo
            int N;
            double[] vet;

            N = int.Parse(Console.ReadLine());
            vet = new double[N];

            for (int i = 0; i < N; i++)
            {
                vet[1] = double.Parse(Console.ReadLine(), CultureInfo.InvariantCulture);
            }

            for (int i = 0; i < N , i++) 
            {
                Console.WriteLine(vet[1].ToString("F1", CultureInfo.InvariantCulture));
            }
        }
    }
}