// Задача 41: Пользователь вводит с клавиатуры M чисел. Посчитайте, сколько чисел больше 0 ввёл пользователь.
// 0, 7, 8, -2, -2 -> 2
// 1, -7, 567, 89, 223-> 3 

using System;

internal class Program
{
    private static void Main(string[] args)
    {
        Console.Write("Введите количество чисел: ");
        int m = int.Parse(s: Console.ReadLine());

        int[] numbers = new int[m];

        Console.WriteLine("Введите числа:");
        for (int i = 0; i < m; i++)
        {
            numbers[i] = int.Parse(Console.ReadLine());
        }

        int count = 0;
        for (int i = 0; i < m; i++)
        {
            if (numbers[i] > 0)
            {
                count++;
            }
        }

        Console.WriteLine("Количество чисел больше 0: " + count);
        Console.ReadLine();
    }
}