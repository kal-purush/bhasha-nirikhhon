using System;
using System.Threading.Tasks;
using System.Threading;

namespace MatrixTPL
{
    class Program
    {
        static void Main(string[] args)
        {
            int[,] mtrx1 = makeMatrix(100,100);
            int[,] mtrx2 = makeMatrix(100,100);

            int[,] resMtrx;
            Parallel.Invoke(
                () =>
                {
                    Console.WriteLine("Task: {0} start. MultiMatrix 1", Task.CurrentId);
                    Thread.Sleep(5000);
                    resMtrx = MultiMatrix(mtrx1,mtrx2);
                    Console.WriteLine("Task: {0} done. MultiMatrix 1", Task.CurrentId);

                },
                () =>
                {
                    Console.WriteLine("Task: {0} start. MultiMatrix 2", Task.CurrentId);
                    Thread.Sleep(6000);
                    resMtrx = MultiMatrix(mtrx1, mtrx2);
                    Console.WriteLine("Task: {0} done. MultiMatrix 2 ", Task.CurrentId);
                });

            Console.ReadKey();
        }

        static int[,] makeMatrix(int height, int width)
        {
            Random rnd = new Random();
            int[,] mtrx = new int[height, width];
            for(int i = 0; i < mtrx.GetLength(0); i++)
            {
                for(int j = 0; j < mtrx.GetLength(1); j++)
                {
                    mtrx[i, j] = rnd.Next(0, 10);
                }
            }
            return mtrx;
        }

        static void Show(int[,] mtrx)
        {
            for (int i = 0; i < mtrx.GetLength(0); i++)
            {
                for (int j = 0; j < mtrx.GetLength(1); j++)
                {
                    Console.Write(mtrx[i,j] + ",");
                }
                Console.WriteLine();
            }
        }

        static int[,] MultiMatrix(int[,] mtrx1, int[,] mtrx2)
        {
            int[,] mtrx = new int[100, 100];
            for (int i = 0; i < mtrx.GetLength(0); i++)
            {
                for (int j = 0; j < mtrx.GetLength(1); j++)
                {
                    mtrx[i, j] = mtrx1[i,j] * mtrx2[j,i];
                }
            }
            return mtrx;
        }
    }
}