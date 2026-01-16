Console.Clear();

int[,] array = GetArray();
PrintArray(array);
BubbleSort(array);
Console.WriteLine();
PrintArray(array);

int[,] GetArray()
{
    int[,] result = new int[5, 5];
    Random random = new Random();
    for (int i = 0; i < result.GetLength(0); i++)
    {
        for (int j = 0; j < result.GetLength(1); j++)
        {
            result[i, j] = random.Next(0, 10);
        }
    }
    return result;
}

void PrintArray(int[,] inArray)
{
    for (int i = 0; i < inArray.GetLength(0); i++)
    {
        for (int j = 0; j < inArray.GetLength(1); j++)
        {
            Console.Write($"{inArray[i, j]} ");
        }
        Console.WriteLine();
    }
}

void BubbleSort(int[,] array)
{
    for (int i = 0; i < array.GetLength(0); i++)
    {
        for (int z = 0; z < array.GetLength(1); z++)
        {
            for (int j = 0; j + 1 < array.GetLength(1); j++)
            {
                if (array[i, j] < array[i, j + 1])
                {
                    int temp = array[i, j];
                    array[i, j] = array[i, j + 1];
                    array[i, j + 1] = temp;
                }
            }
        }
    }
}