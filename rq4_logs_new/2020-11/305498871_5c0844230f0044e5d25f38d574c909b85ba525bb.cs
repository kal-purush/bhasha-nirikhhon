using System;

namespace Task3
{
    public class MyMatrix
    {
        private int[,] _matrix;
        Random _random = new Random();
        public MyMatrix(int m, int n)
        {
            _matrix = new int[m, n];

            for (int i = 0; i < m; i++)
            {

                for (int j = 0; j < n; j++)
                {
                    _matrix[i, j] = _random.Next(50);
                }
            }
        }

        public int Row
        {
            get
            {
                return _matrix.GetLength(0);
            }
        }
        public int Column
        {
            get
            {
                return _matrix.GetLength(1);
            }
        }
        public int this[int index1, int index2]
        {
            get
            {
                return _matrix[index1, index2];
            }
        }

        public void ResizeMatrix(int m, int n)
        {
            int[,] newMatrix = new int[m, n];

            for (int i = 0; i < Math.Min(Row, m); i++)
            {

                for (int j = 0; j < Math.Min(Column, n); j++)
                {
                    newMatrix[i, j] = _matrix[i, j];
                }
            }

            if (m > Row)
            {
                for (int i = Row; i < m; i++)
                {

                    for (int j = 0; j < Column; j++)
                    {
                       newMatrix[i, j] = _random.Next(50);
                    }
                }
            }

            if (n > Column)
            {
                for (int i = 0; i < m; i++)
                {

                    for (int j = Column; j < n; j++)
                    {
                       newMatrix[i, j] = _random.Next(50);
                    }
                }
            }
            _matrix = newMatrix;
        }
        public void ShowPart(int startIndex1, int startIndex2, int endIndex1, int endIndex2)
        {
            if (startIndex1 < 0 || startIndex2 < 0 || endIndex1 > Row || endIndex2 > Column)
            {
                Console.WriteLine("Index is out of a range!");
                return;
            }
            if (startIndex1 < endIndex1 || startIndex2 < endIndex2)
            {

                for (int i = startIndex1; i < endIndex1; i++)
                {

                    for (int j = startIndex2; j < endIndex2; j++)
                    {
                        Console.Write($"{_matrix[i, j]} ");
                    }
                    Console.WriteLine();
                }
            }
            else
            {
                Console.WriteLine("Incorrect input!");
            }
        }
    }
}