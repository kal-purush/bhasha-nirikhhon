// Посчитать кубы чисел,заканчивающихся на нечётную цифру
Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("Производим случайный выбор чисел:");
int [] array = new int [10];
void Random_Num()
{
    for (int i = 0; i < array.Length; i++)
    {
        array[i] = new Random().Next(1,6);
        Console.Write(array[i]+" ");
    }
}
void Odd (int [] array)
{
    for (int i = 0; i < array.Length; i++)
    {
        if (array[i] % 2 == 1) Console.Write($"Куб нечётного числа {array[i]} равен {Cube(array[i])}\n");
    }
}
int Cube (int any)
{
    return any*any*any;
}
Random_Num();
Console.WriteLine();
Odd (array);