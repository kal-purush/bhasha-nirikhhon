using System;

namespace task_01_random_matrix
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            Random random = new Random();
            
            Console.Write("Enter lines matrix count: ");
            int linesCount = int.Parse(Console.ReadLine());
            Console.Write("Enter columns matrix count: ");
            int columnsCount = int.Parse(Console.ReadLine());
            int[,] matrix = new int[linesCount, columnsCount];

            int sum = 0;

            for (int i = 0; i < linesCount; i++)
            {
                for (int j = 0; j < columnsCount; j++)
                {
                    matrix[i, j] = random.Next(1000);
                    sum += matrix[i, j];
                }
            }

            for (int i = 0; i < linesCount; i++)
            {
                for (int j = 0; j < columnsCount; j++)
                {
                    Console.Write($"{matrix[i,j], 5} ");
                }
                Console.WriteLine();
            }
            Console.WriteLine($"Matrix sum: {sum}");
        }
    }
}