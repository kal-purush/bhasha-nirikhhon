int counterForPositiveNumbers = CountingPositiveNumbers();
Console.WriteLine($"Количество положительных чисел: {counterForPositiveNumbers}");

int CountingPositiveNumbers()
{
    int counter = 0;
    Console.WriteLine("Чтобы остановить ввод введте 'stop'!");
    while (true)
    {
        if (strNumber.Equals("stop")) return counter;

        Console.Write("Введите число: ");
        string strNumber = Console.ReadLine()!;

        if (int.TryParse(strNumber, out int i))
        {
            int number = int.Parse(strNumber);
            if (number >= 0) counter++;
        }
    }
}