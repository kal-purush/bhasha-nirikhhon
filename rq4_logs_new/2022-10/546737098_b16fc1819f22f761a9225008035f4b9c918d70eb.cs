// Zadacha34. Задайте массив заполненный случайными положительными трёхзначными числами. 
// Напишите программу, которая покажет количество чётных чисел в массиве.

int size = 5;
int[] numbers = new int[size];
FillArray(numbers, 100, 1000);
PrintArray(numbers);

int count = 0;
for (int i = 0; i < size; i++)
{
	if (numbers[i] % 2 == 0)
		count++;
}
Console.WriteLine("количество четных элементов: " + count);

void FillArray(int[] numbers,
						int minValue = 0,
						int maxValue = 4)
{
	maxValue++;
	int size = numbers.Length;
	Random random = new Random();
	for (int i = 0; i < size; i++)
	{
		numbers[i] = random.Next(minValue, maxValue);
	}
}

void PrintArray(int[] numbers)
{
	int size = numbers.Length;
	Console.WriteLine("Вывод массива:");
	for (int i = 0; i < size; i++)
	{
		Console.Write(numbers[i] + " ");
	}
	Console.WriteLine();

}
