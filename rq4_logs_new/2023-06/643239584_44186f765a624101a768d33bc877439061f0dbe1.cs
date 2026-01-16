// Задача 54: Задайте двумерный массив. Напишите программу, которая упорядочит по убыванию элементы каждой строки двумерного массива.
// Например, задан массив:
// 1 4 7 2
// 5 9 2 3
// 8 4 2 4
// В итоге получается вот такой массив:
// 7 4 2 1
// 9 5 3 2
// 8 4 4 2

int Imput (string message)
{
    Console.WriteLine(message);
    int peremen = Convert.ToInt32(Console.ReadLine());
    return peremen;
}

// int Rows = Imput("Введите количество строк ");
// int Colomns = Imput("Введите количество столбцов ");
// int [,] array = new int [Rows, Colomns];

// int [,] RandomMassiv(int max, int min)
// {
//     for (int i = 0; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             array[i,j] = new Random().Next(max, min+1);
//         }
//     }
//     return array; 
// }

// void OrderTheArray (int [,] number)
// {
//     for (int i = 0; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             for (int d = 0; d < array.GetLength(1)-1; d++)
//             {
//                 if (array[i,d] < array[i, d +1])
//                 {
//                     int swap = array[i,d+1];
//                     array[i,d+1] = array[i,d];
//                     array[i,d] = swap;
//                 }
//             }       
//         }
//     }
// }


// void Print (int [,] array)
// {
//     for (int i = 0; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             Console.Write($"{array[i,j]}  ");
//         }
//         Console.WriteLine();
//     }
// }


// int Min = Imput ("Введите начальние min значение диапазона массива ");
// int Max = Imput ("Введите max значение диапазона массива ");
// Console.WriteLine();
// RandomMassiv(Min, Max);
// Console.WriteLine("Случайный массив:");
// Print(array);
// OrderTheArray(array);
// Console.WriteLine();
// Console.WriteLine("Упорядоченный массив:");
// Print(array);


//----------------------------------------------------------------------------------------------------------------------------------------

// Задача 56: Задайте прямоугольный двумерный массив. Напишите программу, которая будет находить строку с наименьшей суммой элементов.

// Например, задан массив:
// 1 4 7 2
// 5 9 2 3
// 8 4 2 4
// 5 2 6 7
// Программа считает сумму элементов в каждой строке и выдаёт номер строки с наименьшей суммой элементов: 1 строка


// int Rows = Imput("Введите количество строк ");
// int Colomns = Imput("Введите количество столбцов ");
// int [,] array = new int [Rows, Colomns];

// int [,] RandomMassiv(int max, int min)
// {
//     for (int i = 0; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             array[i,j] = new Random().Next(max, min+1);
//         }
//     }
//     return array; 
// }

// void Print (int [,] array)
// {
//     for (int i = 0; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             Console.Write($"{array[i,j]}  ");
//         }
//         Console.WriteLine();
//     }
// }

// void MinSumRows (int [,] number)
// {
//     int sumLine1=0;
//     for (int i = 0; i < array.GetLength(1); i++)
//     {
//         sumLine1= sumLine1 + array[0,i];
//     }
//     int index = 1;
//     int sum = 0;
//     for (int i = 1; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             sum = sum + array[i,j];
//         }
//         if (sumLine1>sum)
//         {
//             sumLine1=sum;
//             index = i+1;
//         }
//         sum=0;
//     }
//     Console.WriteLine($"Строка с наименьшей суммой элементов > {index} (сумма строки = {sumLine1})");
// }

// int Min = Imput ("Введите начальние min значение диапазона массива ");
// int Max = Imput ("Введите max значение диапазона массива ");
// Console.WriteLine();
// RandomMassiv(Min,Max);
// Console.WriteLine("Созданный массив:");
// Print(array);
// Console.WriteLine();
// MinSumRows(array);

//-----------------------------------------------------------------------------------------------------------------------------


// Задача 58: Задайте две матрицы. Напишите программу, которая будет находить произведение двух матриц.
// Например, даны 2 матрицы:
// 2 4 | 3 4
// 3 2 | 3 3
// Результирующая матрица будет:
// 18 20
// 15 18