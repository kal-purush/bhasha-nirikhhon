// Задайте значения M и N. 
//Напишите программу, которая найдёт сумму натуральных элементов в промежутке от M до N.

int m = InputNum("Ввод m:");
int n = InputNum("Ввод n:");
int temp = m;
if (m > n)
{
    m = n;
    n = temp;
}
PrintSum(m, n, temp = 0);
void PrintSum(int m, int n, int sum)
{
    sum = sum + n;
    if (n <= m)
    {
        Console.Write($"сумма= {sum} ");
        return;
    }
    PrintSum(m, n - 1, sum);
}
int InputNum(string input)
{
    Console.Write(input);
    int output = Convert.ToInt32(Console.ReadLine());
    return output;
}