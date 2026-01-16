// Задача 41: Пользователь вводит с клавиатуры M чисел. Посчитайте, сколько чисел больше 0 ввёл пользователь.

// 0, 7, 8, -2, -2 -> 2

// 1, -7, 567, 89, 223-> 4


Console.Write("Введите колличество M чисел: ");
int M = int.Parse(Console.ReadLine()!);
int count = 0;

for (int i = 0; i < M; i++)
{
int a = i+1;
Console.Write($"Введите число {a}: ");
int numbers = int.Parse(Console.ReadLine()!);
if (numbers > 0)
count++;
}
Console.WriteLine($"Чисел > нуля введено {count} штуки");



// Задача 43: Напишите программу, которая найдёт точку пересечения двух прямых, заданных уравнениями
// y = k1 * x + b1, y = k2 * x + b2; значения b1, k1, b2 и k2 задаются пользователем.

// b1 = 2, k1 = 5, b2 = 4, k2 = 9 -> (-0,5; -0,5)

Console.WriteLine("Введи число b1: ");
int b1 = int.Parse(Console.ReadLine()!);

Console.WriteLine("Введи число k1: ");
int k1 = int.Parse(Console.ReadLine()!);

Console.WriteLine("Введи число b2: ");
int b2 = int.Parse(Console.ReadLine()!);

Console.WriteLine("Введи число k2: ");
int k2 = int.Parse(Console.ReadLine()!);

double x = (b2-b1)/(k1-k2);
double y = k2 * x + b2;

Console.WriteLine($"Точка пересечения двух прямых [{x}; {y}]");
    
