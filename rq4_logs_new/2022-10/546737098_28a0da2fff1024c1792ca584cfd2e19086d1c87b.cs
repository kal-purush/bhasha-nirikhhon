// Zadacha38: Задайте массив вещественных чисел. Найдите разницу между максимальным и минимальным элементов массива.
int size = 10;
double[] numbers = new double[size];
FillArrayDouble(numbers, 0, 100);
PrintArrayDouble(numbers);

void FillArrayDouble(double[] numbers,
						int minValue = 0,
						int maxValue = 9)
{
		maxValue++;
	int size = numbers.Length;
	Random random = new Random();
		for (int i = 0; i < size; i++)
	{
		double value = random.NextDouble()*20-10;
		numbers [i] = Math.Round(value, 2);
	}
}

void PrintArrayDouble(double[] numbers)
{
	int size = numbers.Length;
	double max = 0;
	double min = 0;
	double diff =0;
	Console.WriteLine("Вывод массива:");
	for (int i = 0; i < size; i++)
	{
		Console.Write(numbers[i] + "  ");
		if (numbers[i] > max)
		{
			max=numbers[i];
		}
		if (numbers[i] < min)
		{
			min=numbers[i];
		}
	}
		diff = Math.Abs(min)+Math.Abs(max);
		Console.WriteLine();
		Console.WriteLine($"Максимальный элемент массива:{max} минимальный:{min} разница:{Math.Round(diff,2)}");
		Console.WriteLine();

}