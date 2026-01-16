//Общее заполнение для всех задач

Console.WriteLine("Количество строк: ");
int rows = int.Parse(Console.ReadLine()!);
Console.WriteLine("Количество столбцов: ");
int columns = int.Parse(Console.ReadLine()!);

int[,] array = GetArray(rows, columns, 0, 100);
PrintArray(array);
Console.WriteLine();

RowsLine(array);
PrintArray(array);
Console.WriteLine();

OrderDes(array);
PrintArray(array);
Console.WriteLine();

int[,] GetArray(int m, int n, int minValue, int maxValue)
{
    int[,] result = new int[m, n];
    for (int i = 0; i < m; i++)
    {
        for (int j = 0; j < m; j++)
        {
            result[i, j] = new Random().Next(minValue, maxValue + 1);
        }
    }
    return result;
}

void PrintArray(int[,] array)
{
    for (int i = 0; i < array.GetLength(0); i++)
    {
        for (int j = 0; j < array.GetLength(1); j++)
        {
            Console.Write($"{array[i, j]} ");
        }
        Console.WriteLine();
    }
}

//************************************************************************************************************************
// Задача 54: Задайте двумерный массив. Напишите программу, которая упорядочит по убыванию элементы каждой строки двумерного массива.

void OrderDes(int[,] array)
{
  for (int i = 0; i < array.GetLength(0); i++)
  {
    for (int j = 0; j < array.GetLength(1); j++)
    {
      for (int k = 0; k < array.GetLength(1) - 1; k++)
      {
        if (array[i, k] < array[i, k + 1])
        {
          int var = array[i, k + 1];
          array[i, k + 1] = array[i, k];
          array[i, k] = var;
        }
      }
    }
  }
}

//***************************************************************************************************************

//Программа считает сумму элементов в каждой строке и выдаёт номер строки с наименьшей суммой элементов: 1 строка

void RowsLine(int[,] array)
{
    int minValueSum = array.GetLength(0) - 1;
    int sumValue = 0;
    int count = 0;
    for (int i = 0; i < array.GetLength(0); i++)
    {
        for (int j = 0; j < array.GetLength(1); j++)
        {
            sumValue = sumValue + array[i, j];
        }
        if (sumValue < minValueSum)
        {
            minValueSum = sumValue;
            count++;
        }
    }
}
Console.WriteLine($"Строка с наименьшей суммой элементов {count + 1}");