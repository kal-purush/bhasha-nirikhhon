//Задача 1.
//Напишите программу, которая принимает на вход число (А) и выдаёт сумму чисел от 1 до А.
// Через Цыкл FOR
/*
void FindSum1toA (int num_a)
{
    int sumofElem =0;
    for (int current = 1; current <= num_a; current++)
    {
        sumofElem += current; // sumofElem = sumofelem + current
        Console.Write(sumofElem + "");
    }
    Console.WriteLine($"SUm of element from 1 to {num_a} is {sumofElem}");
}
Console.WriteLine("Input your number: ");
int user_num = Convert.ToInt32(Console.ReadLine());

if (user_num > 0)
{
    FindSum1toA(user_num);
}
else
{
    Console.WriteLine("Imbossible value");
}
*/

//Через ЦЫКЛ  WHILE
/*
void FindSumWhile(int user_num)
{
    int current = 0;
    int temp = 1;
    while(temp <= user_num)
    {
        current += temp;
        temp++;
    }
     Console.WriteLine($"SUm of element from 1 to {user_num} is {current}");
} 
Console.WriteLine("Input your number: ");
int user_num = Convert.ToInt32(Console.ReadLine());

if (user_num > 0)
{
    FindSumWhile(user_num);
}
else
{
    Console.WriteLine("Imbossible value");
}
*/

//Напишите программу, которая принимает на вход число и выдаёт количество цифр в числе.
//456 -> 3
//78 -> 2
//89126 -> 5

/*int CountOfdigits(int num_user)
{
    int count = 0;
    while (num_user > 0)
    {
        num_user /= 10; //num_user = num_user /10
        count++;
    }
    return count;
}
Console.WriteLine("Input your number ");
int user_num = Convert.ToInt32(console.ReadLine());

int count_res;


if (user_num >= 0)
    count = CountOfdigits(user_num);
else
{
    int user_num1 = (-1) * user_num;
    count_res = CountOfdigits(user_num1);
}
Console.WriteLine(count_res);
*/


/* 
int FindMulty (int user_n)
{
    int multi = 1;
    for (int count =1; user_n >= count; count++)
    {
        multi *=count;
    }
    return multi;
}

Console.WriteLine("Input your number ");
int user_num = Convert.ToInt32(console.ReadLine());

int multy_res;
if (user_n <= 0) 
    multy_res = 0;
else 
    multy_res = FindMulty(user_num);
Console.WriteLine(multy_res);
*/

//Напишите программу, которая выводит массив из 8 элементов, 
//заполненный нулями и единицами в случайном порядке.

//[1,0,1,1,0,1,0,0]

/*int [] CreateNewArray (int size_array)
{
    int [] random1array = new int[size_array];
}
*/
//Задача 25: Напишите цикл, который принимает на вход два числа (A и B) и возводит число A в натуральную степень B.
/*
//3, 5 -> 243 (3⁵)
//2, 4 -> 16

Задача 27: Напишите программу, которая принимает на вход число и выдаёт сумму цифр в числе.
452 -> 11
82 -> 10
9012 -> 12

Задача 29: Напишите программу, которая задаёт массив из 8 элементов и выводит их на экран.
1, 2, 5, 7, 19 -> [1, 2, 5, 7, 19]
6, 1, 33 -> [6, 1, 33]
*/