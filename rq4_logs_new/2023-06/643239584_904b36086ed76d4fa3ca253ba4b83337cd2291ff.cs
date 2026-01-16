// Задача 64: Задайте значение N. Напишите программу, которая выведет все натуральные числа в промежутке от N до 1. Выполнить с помощью рекурсии.
// N = 5 -> "5, 4, 3, 2, 1"
// N = 8 -> "8, 7, 6, 5, 4, 3, 2, 1"


// Console.Clear();
// Console.WriteLine("Задача 64: Задайте значение N. Напишите программу, которая выведет все натуральные числа в промежутке от N до 1. Выполнить с помощью рекурсии.");

// int Imput (string message)
// {
//     Console.Write(message);
//     int peremen = Convert.ToInt32(Console.ReadLine());
//     return peremen;
// }

// int num = Imput ("Введите число N > ");

// int peremen = 1;
// string PrintNumbers (int numbers)
// {
//     if (numbers == 1) return peremen.ToString();
//     return (numbers+PrintNumbers(numbers-1));
// }
// System.Console.WriteLine();
// System.Console.WriteLine("Натуральные числа в промежутке от N до 1");
// Console.WriteLine($"{PrintNumbers(num)}");


//----------------------------------------------------------------------------------------------------------------------------------------

// Задача 66: Задайте значения M и N. Напишите программу, которая найдёт сумму натуральных элементов в промежутке от M до N.
// M = 1; N = 15 -> 120
// M = 4; N = 8. -> 30

// Console.Clear();
// Console.WriteLine("Задача 66: Задайте значения M и N. Напишите программу, которая найдёт сумму натуральных элементов в промежутке от M до N.");

// int Imput (string message)
// {
//     Console.Write(message);
//     int peremen = Convert.ToInt32(Console.ReadLine());
//     return peremen;
// }
// Console.WriteLine();
// int num1 = Imput("Введите число M > ");
// int num2 = Imput("Введите число N > ");
// int peremen = num2;
// int PrintNumbersInterval (int numders1, int numders2)
// {
//     if (numders1 == numders2)
//     {
//         return numders1;
//     }
//     return (numders1+PrintNumbersInterval(numders1+1,numders2));
// }
// System.Console.WriteLine();
// System.Console.Write($"Сумма натуральных чисел в промежутке от {num1} до {num2} = ");
// System.Console.WriteLine(PrintNumbersInterval(num1,num2));

//----------------------------------------------------------------------------------------------------------------------------------

// Задача 68: Напишите программу вычисления функции Аккермана с помощью рекурсии. Даны два неотрицательных числа m и n.
// m = 2, n = 3 -> A(m,n) = 9
// m = 3, n = 2 -> A(m,n) = 29

Console.Clear();
Console.WriteLine("Задача 68: Напишите программу вычисления функции Аккермана с помощью рекурсии. Даны два неотрицательных числа m и n.");

int Imput (string message)
{
    Console.Write(message);
    int peremen = Convert.ToInt32(Console.ReadLine());
    return peremen;
}

System.Console.WriteLine();
int num1 = Imput ("Введите число m, в промежутке от 0 до 3 > ");
if (num1 == 3) System.Console.Write("Введите число n, от 1 до 10 > "); 
else System.Console.Write("Введите число n > ");

int num2 = Imput(" ");

if (num1<0||num1>3||num2<0) System.Console.WriteLine("Ошибка!");
else if (num1==3 && num2>10) System.Console.WriteLine("Ошибка!");
else
{
    System.Console.WriteLine();
    System.Console.WriteLine(Ackerman (num1,num2));
} 

int Ackerman (int numders1, int numders2)
{
    {
        if (numders1==0)  return numders2+1;
        else if (numders1>0 && numders2==0 ) return Ackerman(numders1-1,1);
        else return Ackerman(numders1-1,Ackerman(numders1,numders2-1));
    }
}