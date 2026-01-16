/// Задайте значения M и N. Напишите программу,
/// которая выведет все натуральные числа в промежутке от M до N. 
/// Использовать рекурсию, не использовать циклы.

class Program
{
    static void PrintNumbersRecursive(int M, int N)
    {
        if (M <= N)
        {
            Console.WriteLine(M);
            PrintNumbersRecursive(M + 1, N);
        }
    }

    static void Main()
    {
        int M = 1; // Замените 1 на желаемое начальное значение
        int N = 10; // Замените 10 на желаемое конечное значение

        PrintNumbersRecursive(M, N);
    }
}