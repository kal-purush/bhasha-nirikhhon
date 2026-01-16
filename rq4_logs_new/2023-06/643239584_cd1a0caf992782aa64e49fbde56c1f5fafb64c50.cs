// Задача 34: Задайте массив заполненный случайными положительными трёхзначными числами. Напишите программу, которая покажет количество чётных чисел в массиве.
// [345, 897, 568, 234] -> 2


int Input (string messang)
{
    Console.Write(messang);
    string peremen1 = Console.ReadLine();
    int peremen2 = Convert.ToInt32(peremen1);
    return peremen2;
}

int [] RandomMassiv (int number, int min, int max)
{
    int [] array = new int [number];
    for (int i = 0; i<number; i++)
    {
        array[i]= new Random().Next(min , max +1);
    } 
    return array;
}

void Print (int [] array )
{
    Console.Write("[");
    for (int i =0; i<array.Length-1; i++)
    {
        Console.Write($"{array[i]}, ");
    }
    Console.Write(array[array.Length-1]);
    Console.Write("]");
}

int  EvenNumber (int [] array)
{
    int Length = array.Length;
    int count =0;
    for (int i =0; i<Length; i++)
    {
        int x = array[i]%2;
        if (x==0)
        {
            count = count+1; 
        }
    }
    return count;  
}

int Length = Input("Введите длинну массива ");
int min = Input("Введите начальное min значение диапазона массива ");
int max = Input("Введите max значение диапазона массива ");
int [] array = RandomMassiv(Length, min, max);
Console.WriteLine();
Print (array);
int Parity = EvenNumber(array);
Console.WriteLine();
Console.WriteLine();
Console.WriteLine($"Четных чисел в массиве = {Parity}");



// -------------------------------------------------------------------------------------------------------------------------------------------------------------------


// Задача 36: Задайте одномерный массив, заполненный случайными числами. Найдите сумму элементов, стоящих на нечётных позициях.
// [3, 7, 23, 12] -> 19
// [-4, -6, 89, 6] -> 0


int  SumOddNumber (int [] array)
{
    int Length = array.Length;
    int count =0;
    for (int i =0; i<Length; i++)
    {
        if (i%2==0)
        {
            count =count+ array[i];
        }
    }
    return count;  
}


// int Length = Input("Введите длинну массива ");
// int min = Input("Введите начальное min значение диапазона массива ");
// int max = Input("Введите max значение диапазона массива ");
// int [] array = RandomMassiv(Length, min, max);
// Console.WriteLine();
// Print (array);
// Console.WriteLine();
// int Sum = SumOddNumber(array);
// Console.WriteLine($"Сумму элементов, стоящих на нечётных позициях = {Sum}");


// -------------------------------------------------------------------------------------------------------------------------------------------------------------------



// Задача 38: Задайте массив вещественных чисел. Найдите разницу между максимальным и минимальным элементов массива.
// [3.22, 4.2, 1.15, 77.15, 65.2] => 77.15 - 1.15 = 76


double []  DoubleRandomMassiv (int number, int min, int max)
{
    Random y = new Random();
    double [] array = new double [number];                                                             
    for (int i = 0; i<number; i++)
    {
        array[i]= Convert.ToDouble(y.Next(min*10 , (max*10)+1)/10.00);
    } 
    return array;
}

void DoublePrint (double [] array )
{
    Console.Write("[");
    for (int i =0; i<array.Length-1; i++)
    {
        Console.Write($"{array[i]}, ");
    }
    Console.Write(array[array.Length-1]);
    Console.Write("]");
}

double MaxNumderMassiv (double [] array)
{
    double max = array[0];
    for (int i =1; i<array.Length; i++)
    {
        if (array[i]>max)
        {
            max = array[i];
        }
    }
    return max;
}  
double MinNumderMassiv (double [] array)
{
    double min = array[0];
    for (int i =1; i<array.Length; i++)
    {
        if (array[i]<min)
        {
            min = array[i];
        }
    }
    return min;
}  

double Difference (double max, double min)
{
    double difference = max - min;
    return difference;
}
// int Length = Input("Введите длинну массива ");
// int min = Input("Введите начальное min значение диапазона массива ");
// int max = Input("Введите max значение диапазона массива ");

// double [] array = DoubleRandomMassiv(Length, min, max);
// DoublePrint (array);
// Console.WriteLine();
// Console.WriteLine();

// double MaxNumder = MaxNumderMassiv(array);
// Console.WriteLine($"Max элемент массива = {MaxNumder}");

// double MinNumder = MinNumderMassiv(array);
// Console.WriteLine($"Min элемент массива = {MinNumder}");

// double difference = Difference(MaxNumder, MinNumder);
// Console.WriteLine($"Разница: {MaxNumder} - ({MinNumder}) = {difference}");