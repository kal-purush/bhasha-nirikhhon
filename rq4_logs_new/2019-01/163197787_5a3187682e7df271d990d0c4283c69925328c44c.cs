using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace HomeworkConsoleApp
{
    class Program
    {
        private static int[,] x, y, z;
        static void Main(string[] args)
        {
            var time = new Stopwatch();
            time.Start();

            Homework(10000, 4);

            time.Stop();
            Console.WriteLine(time.Elapsed);
            Console.ReadKey();
        }

        class MatrixClass
        {
            public int[,] matrix1, matrix2, endMatrix;
            public int startPart, endPart, classNum;

            public MatrixClass(int[,] matrix1, int[,] matrix2, int[,] endMatrix, int startPart, int endPart, int classNum)
            {
                this.matrix1 = matrix1;
                this.matrix2 = matrix2;
                this.endMatrix = endMatrix;
                this.startPart = startPart;
                this.endPart = endPart;
                this.classNum = classNum;
            }
        }

        static async void Homework(int count, int taskCount)
        {
            var filling1 = new Task<int[,]>(() => CreateRandomMatrix(count, new Random()));
            filling1.Start();
            var filling2 = new Task<int[,]>(() => CreateRandomMatrix(count, new Random()));
            filling2.Start();

            x = filling1.Result;
            y = filling2.Result;
            z = new int[count, count];

            var tasks = new List<Task>(taskCount);
            
            double increm = 1 / (double)taskCount;
            double leftIncrem = 0;
            double rightIncrem = increm;

            for (var i = 0; i < taskCount; i++)
            {
                tasks.Add(Task.Run(() => MultiMatrix(
                    new MatrixClass(x, y, z,
                        (int)(count * leftIncrem), 
                        (int)(count * rightIncrem), i))));

                if (i >= taskCount - 1) break;
                leftIncrem += increm;
                rightIncrem += increm;
            }

            var taskComplete = Task.WhenAll(tasks);
            await taskComplete;

            Console.WriteLine("Операция завершена");
        }

        static void MultiMatrix(MatrixClass mat)
        {
            var count = mat.matrix1.GetLength(0);

            for (var i = mat.startPart; i < mat.endPart; i++)
            {
                for (var j = 0; j < count; j++)
                {
                    mat.endMatrix[i, j] = mat.matrix1[i, j] * mat.matrix2[i, j];
                }
            }
        }

        private static int[,] CreateRandomMatrix(int count, Random rnd)
        {
            var temp = new int[count, count];

            for (var i = 0; i < count; i++)
            {
                for (var j = 0; j < count; j++)
                {
                    temp[i, j] = rnd.Next(0, 11);
                }
            }

            return temp;
        }
    }
}