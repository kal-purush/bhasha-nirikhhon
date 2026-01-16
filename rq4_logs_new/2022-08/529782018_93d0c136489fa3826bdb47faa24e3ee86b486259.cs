// Задать массив из 12 элементов , заполненных числами [-9,9]
// Найти сумму положительных/отрицательных элементов массива
Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("Задаём массив из 12 элементов , заполненных числами от (-9 до 9) случайным образом:");
int [] arr = new int [12];
void CreateArray12(int [] array)
{
    for (int i = 0; i < array.Length; i++)
    {
        array[i] = new Random().Next(-9,10);
        Console.Write($"{array[i]} ");
    }
}       
void SumPositiveAndNegativeNumbers(int [] array)
{
    int sum_pos = 0,sum_neg = 0;
    foreach (var item in array)
    {
        if (item > 0) sum_pos += item;
        else sum_neg += item;
    }
    Console.WriteLine();
    Console.WriteLine($"Сумма положительных чисел в данном массиве будет {sum_pos}");
    Console.WriteLine($"Сумма отрицательных чисел в данном массиве будет {sum_neg}");
}
CreateArray12(arr);
SumPositiveAndNegativeNumbers(arr);