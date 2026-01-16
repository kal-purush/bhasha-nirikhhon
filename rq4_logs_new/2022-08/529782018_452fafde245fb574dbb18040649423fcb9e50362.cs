//Найти сумму элементов массива ,стоящих на нечётной позиции
Console.OutputEncoding =System.Text.Encoding.UTF8;
int [] array = new int [6];
void Fillarray()
{
    for (int i = 0; i < array.Length; i++) array[i] = new Random().Next(1,10);
}
void Printarray()
{
    for (int i = 0; i < array.Length; i++) Console.Write(array[i]+" ");    
}
int Sum_Odd(int [] numberes)
{
    int sum = 0;
    for (int i = 0; i < numberes.Length; i++)
    {
        if (i%2==1) sum+=numberes[i];
    }
    return sum; 
}

Fillarray();
Printarray();
Console.WriteLine();
Console.WriteLine($"Сумма элементов массива ,стоящих на нечётной позиции равна {Sum_Odd(array)}");