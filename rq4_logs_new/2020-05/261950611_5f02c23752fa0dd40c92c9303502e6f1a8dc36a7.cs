using System;

namespace Leetcode
{
    class Program
    {
        public static int[][] UpdateMatrix(int[][] matrix)
        {
            int row = matrix.Length;
            if (row == 0)
            {
                return matrix;
            }
            int col = matrix[0].Length;
            int[] col_list = new int[col];
            //set the default distance matrix
            for (int i = 0; i < col; i++)
            {
                col_list[i] = row * col;
            }
            int[][] dist = new int[row][];
            for (int i = 0; i < row; i++)
            {
                dist[i] = col_list;
            }
            Console.WriteLine(dist[2][1]);
            for (int i = 0; i < row; i++)
            {
                for (int j = 0; j < col; j++)
                {
                    if (matrix[i][j] == 0)
                    {
                        dist[i][j] = 0;
                        Console.WriteLine(dist[2][2]);
                    }
                    else
                    {
                        if (i > 0)
                            dist[i][j] = Math.Min(dist[i][j], dist[i - 1][j] + 1);
                        if (j > 0)
                            dist[i][j] = Math.Min(dist[i][j], dist[i][j - 1] + 1);
                    }
                }
            }
            //Console.WriteLine(dist[2][1]);
            for (int i = row - 1; i >= 0; i--)
            {
                for (int j = col - 1; j >= 0; j--)
                {
                    if (i < row - 1)
                        dist[i][j] = Math.Min(dist[i][j], dist[i + 1][j] + 1);
                    if (j < col - 1)
                        dist[i][j] = Math.Min(dist[i][j], dist[i][j + 1] + 1);
                }
            }
            return dist;
        }

        static void Main(string[] args)
        {
            int[][] matrix = new int[3][];
            matrix[0] = new int[3] { 0, 0, 0 };
            matrix[1] = new int[3] { 0, 1, 0 };
            matrix[2] = new int[3] { 1, 1, 1 };
            Console.WriteLine(UpdateMatrix(matrix)[2][1]);
        }
    }
}