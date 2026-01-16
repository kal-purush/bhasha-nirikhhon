//Задайте произвольный массив. 
//Выведете его элементы, начиная с конца. Использовать рекурсию, не использовать циклы.

int[] CreateArray (int[] array)
{
    for (int i = 0; i < array.Length; i++)
    {
        array[i] = new Random().Next(1, 10);
        }
    return array;
}

void PrintReverse(int i, int[] array)
{
    if (i < 0)
    {
        return;
    }
    else
    {
        Console.Write($"{array[i]}, ");
        PrintReverse(i - 1, array);
    }
}

int number = 5;
int[] array = new int[number];
array = CreateArray(array);
Console.WriteLine(String.Join(", ", array));
int i = array.Length - 1;
PrintReverse(i, array);