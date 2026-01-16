// Напишите программу, которая на вход принимает два числа и проверяет, является ли первое число квадратом второго.

// Console.Write("Введите первое число: ");
// int num1 = Convert.ToInt32(Console.ReadLine());

// Console.Write("Введите второе число: ");
// int num2 = Convert.ToInt32(Console.ReadLine());

// int kvadrat = num2 * num2;

// if(num1 == kvadrat)
// {
//     Console.WriteLine("Первое число является квадратом второго");
// }
// else 
// {
//     Console.WriteLine("Первое число НЕ является квадратом второго");
// }

//Напишите программу, которая на вход принимает одно число (N), а на выходе показывает все целые числа в промежутке от -N до N.

Console.Write("Введите число: ");
int num = Convert.ToInt32(Console.ReadLine());

int current = num * (-1);

while (current <= num)
{
    Console.Write(current);
    current = current +1;
}