        int x = Convert.ToInt32(Console.ReadLine());
        int[,] matrix = new int[x, x];

        for (int i = 0; i < x; i++)
        {
            for (int j = 0; j < x; j++)
            {
              matrix[i, j] = Convert.ToInt32(Console.ReadLine());
            }
        }

    

        static bool isMatrixSymmetric(int[,] matrix)
        {
        int n = matrix.GetLength(0);
        for (int i = 0; i < n; i++)
            {
                for (int j = i + 1; j < n; j++)
                {
                    if (matrix[i, j] != matrix[j, i])
                    {
                    return false;
                    }
                }

            }
            return true;
        }
        
        for(int i=0; i<x; i++)
{
        for(int j = 0; j<x; j++)
    {
        Console.Write($"{matrix[i, j]}");
    }
    Console.WriteLine();
}

if (isMatrixSymmetric(matrix))
{
    Console.WriteLine("YES");
}
else
{
    Console.WriteLine("NO");
}