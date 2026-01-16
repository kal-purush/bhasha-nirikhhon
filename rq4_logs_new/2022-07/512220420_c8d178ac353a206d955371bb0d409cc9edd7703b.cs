/*Задача 4: Напишите программу, которая принимает на вход три числа и выдаёт 
максимальное из этих чисел.*/

// First solution
void FirstSolution()
{
Console.WriteLine ("Insert first number: ");
int number1 = Convert.ToInt32(Console.ReadLine());
Console.WriteLine ("Insert second number; ");
int number2 = Convert.ToInt32(Console.ReadLine());
Console.WriteLine ("Insert third number: ");
int number3 = Convert.ToInt32(Console.ReadLine());
int max = number1;

if (number2 > number1)
{
    max = number2;
}
if (number3 > max)
{
    max = number3;
}

Console.WriteLine($"max = {max}");
}

// Second solution
void SecondSolution()
{
Console.WriteLine ("Insert first number: ");
int number1 = Convert.ToInt32(Console.ReadLine());
Console.WriteLine ("Insert second number; ");
int number2 = Convert.ToInt32(Console.ReadLine());
Console.WriteLine ("Insert third number: ");
int number3 = Convert.ToInt32(Console.ReadLine());
int max = Math.Max(number1, Math.Max(number2, number3));

Console.WriteLine($"max = {max}");
}

//FirstSolution();
SecondSolution();