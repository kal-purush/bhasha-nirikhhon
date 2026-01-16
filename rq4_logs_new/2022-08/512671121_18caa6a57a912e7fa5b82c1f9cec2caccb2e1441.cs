// Задайте двумерный массив. 
//Напишите программу, которая упорядочит по убыванию элементы каждой строки двумерного массива.

void Print(int[] arr)
{
    int row_size = arr.GetLength(0);
    int column_size = arr.GetLength(1);

    for (int i = 0; i < row_size; i++)
    {
        for (int j = 0; j < column_size; j++)
            Console.Write($" {arr[i, j],3} ");
        Console.WriteLine();
    }
    Console.WriteLine();
}

int[] MassNums(int row, int column, int from, int to)
{
    int[,] arr = new int[row, column];

    for (int i = 0; i < row; i++)
        for (int j = 0; j < column; j++)
            arr[i, j] = new Random().Next(from, to);
    return arr;
}
void Ordmin(int[,] arr)
{
    int row_size = arr.GetLength(0);
    int column_size = arr.GetLength(1);

    for (int i = 0; i <= row_size; i++)
    {
        for (int j = 0; j < column_size; j++)
        {
            for (int k = 0; k < column_size; k++)
                if (arr[i, k] < arr[i, k + 1])
                    (arr[i, k], arr[i, k + 1]) = (arr[i, k + 1],arr[i,k]);
        }
    }
}

Console.Write("введите строку: ");
int row = int.Parse(Console.ReadLine());
Console.Write("введите столбец: ");