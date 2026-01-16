using System;
using System.Threading.Tasks;

namespace Lesson6_matrix
{
    class Program
    {
        static async Task Main(string[] args)
        {
            int[,] matrix1 = new int[100, 100];
            int[,] matrix2 = new int[100,100];
            Random random = new Random();
            for (int i = 0; i < 100; i++)
            {
                for (int z = 0; z < 100; z++)
                {
                    matrix1[i, z] = random.Next(1, 10);
                    matrix2[i, z] = random.Next(1, 10);
                }
            }
            Console.WriteLine("matrix1:");
            PrintMatrix(matrix1);
            Console.WriteLine("matrix2: ");
            PrintMatrix(matrix2);
            var result = await MultipMatrix(matrix1, matrix2);
            Console.WriteLine("multiplication matrix:");
            PrintMatrix(result);
            Console.ReadKey();
        }
        static private void PrintMatrix(int [,] a)
        {
            for (int i = 0; i < a.GetLength(0); i++)
            {
                Console.WriteLine($"{a[i, 0]}");
                for (int z = 1; z < a.GetLength(1); z++)
                {
                    Console.Write($" {a[i, z]}");
                }
            }
        }
        static private Task<int[,]> MultipMatrix(int [,] a,int [,] b)
        {
            if (a.GetLength(0) != b.GetLength(0) || a.GetLength(1) != b.GetLength(1))
            {
                throw new Exception("Массивы разной размерности, данный метод работает только с массивами одинаковой размерности");
            }
            return Task.Run(() =>
            {
                int[,] resultMultip = new int[a.GetLength(0), a.GetLength(1)];
                for (int i = 0; i < a.GetLength(0); i++)
                {
                    for (int z = 0; z < b.GetLength(1); z++)
                    {
                        resultMultip[i, z] = a[i, z] * b[i, z];
                    }
                }
                return resultMultip;
            });            
        }
    }
}