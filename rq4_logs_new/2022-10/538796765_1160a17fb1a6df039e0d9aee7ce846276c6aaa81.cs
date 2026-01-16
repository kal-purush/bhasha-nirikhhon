// Задача 47. Задайте двумерный массив размером m×n, заполненный случайными целыми числами.


Console.WriteLine("Количество строк: ");
int rows=int.Parse(Console.ReadLine()!);

Console.WriteLine("Количество столбцов: ");
int columns =int.Parse(Console.ReadLine()!);

int[,] array = GetArray(rows, columns, -100 , 100);
PrintArray(array);

int[,] GetArray(int m, int n, int minValue, int maxValue)
{
    int[,] result=new int [m,n];
    for(int i=0; i<m; i++)
    {
        for(int j=0; j<m; j++)
        {
            result[i,j]=new Random().Next(minValue,maxValue+1);
        }
    }
    return result;
}

void PrintArray(int[,] array)
{
    for(int i=0; i<array.GetLength(0); i++)
    {
        for(int j=0; j<array.GetLength(1); j++)
        {
            Console.Write($"{array[i,j]} ");
        }
        Console.WriteLine();
    }
}



//Задача 50. Напишите программу, которая на вход принимает позиции элемента в двумерном массиве, и возвращает значение этого элемента или же указание, что такого элемента нет.

//Например, задан массив:




Console.WriteLine("Позиция по строке ");
int rows=int.Parse(Console.ReadLine()!);

Console.WriteLine("Позиция по столбцам: ");
int columns =int.Parse(Console.ReadLine()!);

int[,] array = GetArray(rows, columns, -100 , 100);
PrintArray(array);

int[,] GetArray(int rows, int columns, int minValue, int maxValue)
{
    for(int i=0; i<array.GetLength(0); i++)
    {
        for(int j=0; j<array.GetLength(1); j++)
        {
            Console.Write($"{array[i,j]} ");
        }
        Console.WriteLine();
    }
}

if (rows > array.GetLength(0) || columns > array.GetLength(1))
{
 Console.WriteLine("такого элемента нет");
}
else
{
 Console.WriteLine($"найдено число {array[rows,columns]}");
}



//Задача 52. Задайте двумерный массив из целых чисел. Найдите среднее арифметическое элементов в каждом столбце.



int rows=new Random().Next(0,10);
int columns =new Random().Next(0,10);

Console.WriteLine($"Получили строк - {rows}");
Console.WriteLine($"Получили столбцов - {columns}");
Console.WriteLine();


int[,] array = GetArray(rows, columns, 0, 10);
PrintArray(array);
Console.WriteLine();
int SumNum = NewGetArray(array);
Console.WriteLine($"Среднее арифметическое по столбцам - {SumNum}");

int[,] GetArray(int row, int column, int minValue, int maxValue)
{
    int[,] result=new int [row, column];
    for(int i=0; i<row; i++)
    {
        for(int j=0; j<column; j++)
        {
            result[i,j]=new Random().Next(minValue,maxValue);
        }
    }
    return result;
}

void NewGetArray(int[,] array); 
for (int j = 0; j < numbers.GetLength(1); j++)
{
 double avarage = 0;
 for (int i = 0; i < array.GetLength(0); i++)
    {
 avarage = (avarage + array[i, j]);
    }
 avarage = avarage / rows;
 Console.Write(avarage + "; ");
}



