// Задача 2: Напишите программу, которая на вход принимает два числа и выдаёт, какое число большее, а какое меньшее.

// a = 5; b = 7 -> max = 7
// a = 2 b = 10 -> max = 10
// a = -9 b = -3 -> max = -3

// Console.Write("Введите первое число: ");
// int num1 = Convert.ToInt32(Console.ReadLine());
// Console.Write("Введите второе число: ");
// int num2 = Convert.ToInt32(Console.ReadLine());
// if (num1 > num2)
//     Console.WriteLine("max = " +num1);
// else if (num1 == num2)
//     Console.WriteLine("Вы ввели одинаковые числа");
// else
//     Console.WriteLine("max = " +num2);


// Задача 4: Напишите программу, которая принимает на вход три числа и выдаёт максимальное из этих чисел.

// 2, 3, 7 -> 7
// 44 5 78 -> 78
// 22 3 9 -> 22

// Console.Write("Введите первое число: ");
// int a = Convert.ToInt32(Console.ReadLine());
// Console.Write("Ведите второе число: ");
// int b = Convert.ToInt32(Console.ReadLine());
// Console.Write("Введите третье число: ");
// int c = Convert.ToInt32(Console.ReadLine());
// int max = a;
// if (b > max) max = b;
// if (c > max) max = c;
// Console.WriteLine("max = " +max);



// Задача 6: Напишите программу, которая на вход принимает число и выдаёт, является ли число чётным (делится ли оно на два без остатка).

// 4 -> да
// -3 -> нет
// 7 -> нет

// Console.Write("Введите целое число: ");
// int num = Convert.ToInt32(Console.ReadLine());
// if (num % 2 == 0)
//     Console.WriteLine("Число четное");
// else 
//     Console.WriteLine("Число нечетное");



// Задача 8: Напишите программу, которая на вход принимает число (N), а на выходе показывает все чётные числа от 1 до N.

// 5 -> 2, 4
// 8 -> 2, 4, 6, 8

// Console.Write("Введите целое положительное число: ");
// int n = Convert.ToInt32(Console.ReadLine());
// int n1 = 1;
// while (n1 <= n)
// {
//     if (n1 % 2 == 0)
//     {
//         Console.Write(n1 + " ");
//     }
//     n1 = n1 + 1;
// }