using System;

namespace ExercicioMatriz
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Seu programa terá quantas linhas? ");
            int M = int.Parse(Console.ReadLine());

            Console.Write("Seu programa terá quantas colunas? ");
            int N = int.Parse(Console.ReadLine());

            int[,] mat = new int[M, N];

            

            for (int i = 0; i < M; i++)
            {
                Console.Write("Digite três valores: ");
                string[] valores = Console.ReadLine().Split(' ');

                for (int j = 0; j < N; j++)
                {
                    mat[i, j] = int.Parse(valores[j]);
                }
            }

            Console.WriteLine();
            Console.WriteLine("VALORES DA MATRIZ");

            for (int i = 0;  i < M; i++)
            {
                for (int j = 0; j < N; j++)
                {
                    Console.Write(mat[i, j] + " ");
                    if (j == N-1)
                    {
                        Console.WriteLine();
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("DIAGONAL PRINCIPAL");

            for (int i = 0; i < M; i++)
            {
                for (int j = 0; j < N; j++)
                {
                    
                    if (i == j)
                    {
                        Console.Write(mat[i, j] + " ");
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("NÚMEROS NEGATIVOS");

            for (int i = 0; i < M; i++)
            {
                for (int j = 0; j < N; j++)
                {

                    if (mat[i,j] < 0)
                    {
                        Console.Write(mat[i, j] + " ");
                    }
                }
            }


        }
    }
}