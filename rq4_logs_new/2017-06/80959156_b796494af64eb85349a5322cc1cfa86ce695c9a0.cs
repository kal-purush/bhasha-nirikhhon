using System;

namespace task_DEV_8
{
    /// <summary>
    /// Class of entity matrix, which include array of double elements
    /// </summary>
    class Matrix
    {
        private double[,] content;

        public Matrix(double[,] content)
        {
            this.content = content;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="firstMatrix"></param>
        /// <param name="secondMatrix"></param>
        /// <returns></returns>
        public static Matrix operator *(Matrix firstMatrix, Matrix secondMatrix)
        {
            int resultColumnCount = firstMatrix.content.GetLength(0);
            int resultLineCount = secondMatrix.content.GetLength(1);
            if (firstMatrix.content.GetLength(1) != secondMatrix.content.GetLength(0))
            {
                throw new InvalidOperationException("Matrix can't multiply!");
            }
            double[,] result = new double[resultColumnCount, resultLineCount];
            for (int i = 0; i < resultColumnCount; i++)
            {
                for (int j = 0; j < resultLineCount; j++)
                {
                    for (int z = 0; z < firstMatrix.content.GetLength(1); z++)
                    {
                        result[i, j] += firstMatrix.content[i, z] * secondMatrix.content[z, j];
                    }
                }
            }
            return new Matrix(result);
        }

        public void Print()
        {
            for (int i = 0; i < content.GetLength(0); i++)
            {
                for (int j = 0; j < content.GetLength(1); j++)
                {
                    Console.Write("{0}  ", content[i, j]);
                }
                Console.WriteLine();
            }
        }
    }
}