// Задайте массив из 12 элементов, заполненный случайными числами из промежутка [-9, 9].
// Найдите сумму отрицательных и положительных элементов массива.
// Например, в массиве [3,9,-8,1,0,-7,2,-1,8,-3,-1,6] сумма положительных чисел равна 29,
// сумма отрицательных равна -20.

void Print(int[] arr)
{
int size = arr.Length;

for (int i = 0; i < size; i++)
Console.Write($"{arr[i]} ");
Console.WriteLine();
}

int[] EightMass(int size, int from, int to)
{
int[] arr = new int[size];

for (int i = 0; i < size; i++)
arr[i] = new Random().Next(from, to + 1);
return arr;
}

void SumPosNeg(int[] arr)
{
int pos, neg;
pos = neg = 0;

for (int i = 0; i < arr.Length; i++)
{
if (arr[i] >= 0)
pos += arr[i];
else
neg += arr[i];
}
Console.WriteLine($"Pos: {pos}, neg: {neg}");
}

int num = int.Parse(Console.ReadLine()!);
int start = int.Parse(Console.ReadLine()!);
int stop = int.Parse(Console.ReadLine()!);

int[] mass = EightMass(num, start, stop);
Print(mass);
SumPosNeg(mass);