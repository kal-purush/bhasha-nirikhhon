// Написать программу вычисления произведения чисел от 1 до N
//------------------------------------------------------------------------------------
//Способ 1:

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("Введите число ");
int number = int.Parse(Console.ReadLine());
int [] array = new int [number];

void Factorial(int num)
{
    int mult = 1;
    for (int i = 1; i <= num; i++)
    {
        mult*=i;
    }
    Console.WriteLine($"Произведение чисел от 1 до {num} равно {mult}");
}
//-----------------------------------------------------------------------------------
//Способ 2: (если числа от 1 до N будут в произвольном порядке и могут повторяться)

void Abitrory_numbers (int num2)//          печать случайных чисел массива от 1 до N
{   
    for (int i = 0; i < num2; i++)
    {
        array[i] = new Random().Next(1,num2+1);
        Console.Write(array[i]+" ");
    }
}
int Mult_numbers_array(int [] array) //     подсчёт произведения случайных чисел от 1 до N в массиве
{
    int res = array[0];
    for (int i = 1; i < array.Length; i++)
    {
        res *= array[i];
    }
    return res;
}
//------------------------------------------------------------------------------------

Factorial(number);
Console.WriteLine();
Abitrory_numbers(number);
Console.WriteLine();
Console.WriteLine( $"Произведение случайных чисел от 1 до {number} равно {Mult_numbers_array(array)}");