// 2. Задайте массив. Напишите программу, которая определяет,
// присутствует ли заданное число в массиве.

Console.Write("Кол-во элементов:");
int count = int.Parse(Console.ReadLine()!);
Console.Write("Начало диапозона:");
int a = int.Parse(Console.ReadLine()!);
Console.Write("Конец диапозона:");
int b = int.Parse(Console.ReadLine()!);

int[] MakeArray(int c, int m, int n)
{
    int[] array = new int[c];

    for (int i = 0; i < array.Length; i++) array[i] = new Random().Next(m, n + 1);
    return array;
}

void PrintArray(int[] arr)
{
    for (int i = 0; i < arr.Length; i++) Console.Write($"{arr[i]} ");
    Console.WriteLine();
}

int[] array = MakeArray(count, a, b);
PrintArray(array);
////////////////////////////////////////////

bool FindNumber(int[] arr, int n)
{

    for (int i = 0; i < arr.Length; i++)
    {
        if (arr[i] == n)
        {
            return true;
        }
    }
    return false;
}

Console.Write("Введите число:");
int n = int.Parse(Console.ReadLine()!);

Console.WriteLine(FindNumber(array, n));