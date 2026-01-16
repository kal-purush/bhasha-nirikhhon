// Задача 41: Пользователь вводит с клавиатуры M чисел. Посчитайте, сколько чисел больше 0 ввёл пользователь.
// 0, 7, 8, -2, -2 -> 2
// 1, -7, 567, 89, 223-> 3

int Imput (string message){
    Console.WriteLine(message);
    string peremen1 = Console.ReadLine();
    int peremen2 = Convert.ToInt32(peremen1);
    return peremen2;
}

int [] ImputMassiv (int numder)
{
    int [] array = new int [numder];
    for (int i = 0; i < numder; i++)
    {
        array[i]=Imput("Введите число в массив");
    }
    return array;
}

void PrintMassiv (int [] array)
{
    Console.Write("[");
    for (int i = 0; i < array.Length-1 ; i++)
    {
        Console.Write($"{array[i]}, ");
    }
    Console.Write(array[array.Length-1]);
    Console.Write("]");
}


void Count (int [] array)
{
    int count = 0;
    for (int i = 0; i < array.Length; i++)
    {
        if (array[i]>0) count++;
    }
    Console.Write($" > {count}");
} 

// int LengthMassiv = new Random().Next(3,10);
// int [] x = ImputMassiv(LengthMassiv);
// PrintMassiv(x);
// Count(x);



//----------------------------------------------------------------------------------------------------------------------------

// Задача 43: Напишите программу, которая найдёт точку пересечения двух прямых, заданных уравнениями y = k1 * x + b1, y = k2 * x + b2; 
// значения b1, k1, b2 и k2 задаются пользователем.
// b1 = 2, k1 = 5, b2 = 4, k2 = 9 -> (-0,5; -0,5)


void Intersection (double b1, double b2, double k1, double k2)
{
    if (k1==k2)
    {
        Console.WriteLine("Прямые параллельны!");
    }
    else 
    {
        double x = (b2-b1)/(k1-k2);
        Console.WriteLine(x);
        double y = k1*x+b1;
        Console.WriteLine(y);
        Console.WriteLine($"Точка пересечения двух прямых [{x},{y}]");
    }
}

double b1 = Imput ("Введите B1");
double k1 = Imput ("Введите K1");
double b2 = Imput ("Введите B2");
double k2 = Imput ("Введите K2");
Intersection (b1,b2,k1,k2);