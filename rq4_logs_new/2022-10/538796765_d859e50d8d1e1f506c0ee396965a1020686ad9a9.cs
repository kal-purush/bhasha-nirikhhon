//Все массивы от 5-ти элементов

//Задача 34: Задайте массив заполненный случайными положительными трёхзначными числами.Напишите программу, которая покажет количество чётных чисел в массиве.
//[345, 897, 568, 234] -> 2


int[] numbers = new int[5];

for(int j = 0; j < numbers.Length; j++)
{
numbers[j] = new Random().Next(-999,1000);
}
for(int j = 0; j < numbers.Length; j++)
{
Console.Write($"[{numbers[j]}]");
}

int count = 0;
for (int j = 0; j < numbers.Length; j++)

if (numbers[j]%2 == 0)
count++;

Console.Write($"  Ответ -> {count}");




//Задача 36: Задайте одномерный массив, заполненный случайными числами. Найдите сумму элементов, стоящих на нечётных позициях.

//[3, 7, 23, 12] -> 19

//[-4, -6, 89, 6] -> 0


int[] numbers = new int[5];

for(int i = 0; i < numbers.Length; i++)
{
numbers[i] = new Random().Next(-100,100);
}
   
for(int i = 0; i < numbers.Length; i++)
{
Console.Write(numbers[i] + ",");
}
      
int sum = 0;
for (int j = 0; j < numbers.Length; j+=2)
        
sum = sum + numbers[j];

Console.WriteLine($"    Ответ -> {sum}");




//Задача 38: Задайте массив вещественных чисел. Найдите разницу между максимальным и минимальным элементов массива.

//[3 7 22 2 78] -> 76


double[] numbers = new double[5];
double min = Int32.MaxValue;
double max = Int32.MinValue;
for(int j = 0; j < numbers.Length; j++)
    {
    numbers[j] = new Random().Next(-100,100)/10;
    }
    
for(int j = 0; j < numbers.Length; j++)
    {
    Console.Write(numbers[j] + ";");
    }

for (int i = 0; i < numbers.Length; i++)
{
    if (numbers[i] > max)
        {
        max = numbers[i];
        }
    if (numbers[i] < min)
        {
        min = numbers[i];
        }
}
double x = max-min;
Console.WriteLine($"    Ответ ->  Разница между максимальным и минимальным числом = {x}");