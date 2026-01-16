// 1. Напишите программу, которая перевернёт одномерный массив
// (последний элемент будет на первом месте, а первый - на последнем и т.д.)

void Print(int[] arr)
{
int size = arr.Length;

for (int i = 0; i < size; i++)
Console.Write($"{arr[i]} ");
Console.WriteLine();
}

int[] MassNums(int size, int from, int to)
{
int[] arr = new int[size];

for (int i = 0; i < size; i++)
arr[i] = new Random().Next(from, to + 1);

return arr;
}

void RevMass(int[] arr)
{
int size = arr.Length;
for (int i = 0; i < size / 2; i++)
(arr[i], arr[size - i - 1]) = (arr[size - i - 1], arr[i]);
}

int num = int.Parse(Console.ReadLine()!);
int start = int.Parse(Console.ReadLine()!);
int stop = int.Parse(Console.ReadLine()!);

int[] mass = MassNums(num, start, stop);
Print(mass);
RevMass(mass);
Print(mass);