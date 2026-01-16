// Задача 64: Задайте значение N. Напишите программу, которая выведет все натуральные числа в промежутке от N до 1. Выполнить с помощью рекурсии.

Console.Write("Введи число N:");
int n=int.Parse(Console.ReadLine());

string Recurs (int n, int minValue)
{
    if(n == minValue)
    {
        return n.ToString();
    }
    return (n + "" + Recurs(n-1,minValue));
}

Console.WriteLine(Recurs(n, 1));

//********************************************************************************************************************

// Задача 66: Задайте значения M и N. Напишите программу, которая найдёт сумму натуральных элементов в промежутке от M до N.

Console.Write("Введи число M:");
int m=int.Parse(Console.ReadLine());
 Console.Write("Введи число N:");
int n=int.Parse(Console.ReadLine());

int Recurs(int m, int n)
{
    int sum = m;
    if (m == n)
        return 0;
    else
    {
        m++;
        sum = m + Recurs(m, n);
        return sum;
    }
}

Console.WriteLine(Recurs(m - 1, n));

//********************************************************************************************************************
//Задача 68: Напишите программу вычисления функции Аккермана с помощью рекурсии. Даны два неотрицательных числа m и n.

Console.Write("Введи число M:");
int m=int.Parse(Console.ReadLine());
Console.Write("Введи число N:");
int n=int.Parse(Console.ReadLine());

int Akkerman (int m, int n)
{
    if (m == 0)
    {
    return n + 1;
    }
    else (n == 0 && m > 0)
    {
        return Akkerman(m - 1, 1);
    }
    else
    {
        return (Akkerman(m - 1, Akkerman(m, n - 1)));
    }
}

Console.WriteLine($"Функция аккермана для чисел {m} и {n} равняется {Akkerman(m, n)}");