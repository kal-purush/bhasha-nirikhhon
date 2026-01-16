// Zadacha36. Задайте одномерный массив, заполненный случайными числами. Найдите сумму элементов с НЕчётными индексами.
int size = 4;
int[] numbers = new int[size];
FillArray(numbers, 0, 100);
PrintArray(numbers);

int sum = 0;
for (int i = 0; i < size; i++)
{
		if (i%2==1) sum+=numbers[i];
}
Console.WriteLine("Сумма элементов на НЕчетных позициях: " + sum);

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
