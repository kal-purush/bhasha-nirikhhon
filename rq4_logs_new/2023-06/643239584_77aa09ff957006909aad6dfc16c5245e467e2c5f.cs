// Задайте двумерный массив и поменяйте местами первый и последний элемент в стороке массива

// int Imput (string message)
// {
//     Console.Write(message);
//     int peremen = Convert.ToInt32(Console.ReadLine());
//     return peremen;
// }
// int rows = Imput ("Введите кол-во срочек > ");
// int columns = Imput ("Введите кол-во столбцов > ");

// int [,] array = new int [rows, columns];

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

// void RandomMassiv (int [,] array, int min, int max)
// {
//    for (int i = 0; i < array.GetLength(0); i++)
//     {
//         for (int j = 0; j < array.GetLength(1); j++)
//         {
//             array[i,j] = new Random().Next(min, max+1);
//         }
//     }

// }

// void ChangArray (int [,] array)
// { 
//     for (int i = 0; i < array.GetLength(0); i++)
//     {
//         int temp = array[i,0];
//         array[i,0] = array[i,array.GetLength(1)-1];
//         array[i,array.GetLength(1)-1]=temp;
//     }
// }

// int Min = Imput ("Введите min значение диапозона массива > ");
// int Max = Imput ("Введите max значение диапозона массива > ");
// RandomMassiv(array, Min, Max);
// Console.WriteLine();
// Console.WriteLine("Полученный массив:");
// Print (array);
// ChangArray(array);
// Console.WriteLine();
// Console.WriteLine("Массив где первый элемент равен последниму, а последний первому:");
// Print (array);

//-----------------------------------------------------------------------------------------


// Напишите программу которая поменят местами строку и столбец 


int Imput (string message)
{
    Console.Write(message);
    int peremen = Convert.ToInt32(Console.ReadLine());
    return peremen;
}
int rows = Imput ("Введите размер квадратного массива > ");

int [,] array = new int [rows, rows];
int [,] array2 = new int [rows,rows];
void Print (int [,] array)
{
    for (int i = 0; i < array.GetLength(0); i++)
    {
        for (int j = 0; j < array.GetLength(1); j++)
        {
            Console.Write($"{array[i,j]}  ");
        }
        Console.WriteLine();

    }
}

void RandomMassiv (int [,] array, int min, int max)
{
   for (int i = 0; i < array.GetLength(0); i++)
    {
        for (int j = 0; j < array.GetLength(1); j++)
        {
            array[i,j] = new Random().Next(min, max+1);
        }
    }

}

void ChangArray (int [,] array)
{ 
    for (int i = 0; i < array.GetLength(0); i++)
    {
        for (int j = 0; j < array.GetLength(1); j++)
        {
            array2[j,i] = array[i,j];
        }
    }
}

int Min = Imput ("Введите min значение диапозона массива > ");
int Max = Imput ("Введите max значение диапозона массива > ");
RandomMassiv(array, Min, Max);
Console.WriteLine();
Console.WriteLine("Полученный массив:");
Print (array);
ChangArray(array);
Console.WriteLine();
Console.WriteLine("Массив где первый элемент равен последниму, а последний первому:");
Print (array2);


