//Напишите программу, которая принимает на вход пятизначное число и проверяет, является ли оно палиндромом.
//14212 -> нет
//12821 -> да
//23432 -> да

void Palin(int num)
{
    int num_1_2 = num / 1000;
    int num_5 = num % 10;
    int num_4 = num / 10 % 10;

    if (num_1_2 == num_5 *10 + num_4)
    Console.WriteLine($"да, {num} палиндром");
    else
    Console.WriteLine($"нет, {num} не палиндром");
}

Console.WriteLine("Entry a five-digit number ");
int val = int.Parse(Console.ReadLine()!);

Palin(val);