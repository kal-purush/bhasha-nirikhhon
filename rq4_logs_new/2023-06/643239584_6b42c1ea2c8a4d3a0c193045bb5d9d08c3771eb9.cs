// Написать программу которая на вход принимает число и выдает сумму чисел этого числа 
// Например 4 = 1+2+3+4 =10

// int Print (string Message)
// {
//     Console.WriteLine(Message);
//     string peremen1 = Console.ReadLine();
//     int peremen2 = Convert.ToInt32(peremen1);
//     return peremen2; 
// }
// int Sum (int x)
// {
//     int y = 0;
//     int v = 0;
//     for (int i = x; i > 0; i--)
//     {
//         v = i-1;
//         y = y+i;
//     }
//     return y;

// }
// int X = Print("Введите число");
// int y = Sum(X);
// Console.WriteLine(y);

//-------------------------------------------------------------------------

//Напишите программу которая на вход принимает число
// и выдает количество цифр в числе


// int Print (string Message)
// {
//     Console.WriteLine(Message);
//     string peremen1 = Console.ReadLine();
//     int peremen2 = Convert.ToInt32(peremen1);
//     return peremen2; 
// }

// int Sun (int x)
// {
//     int y = 0;
//     while(x>0)
//     { 
//         x = x/10;
//         y = y+1;
//     }
//     return y; 
// }
// int number = Print("Введите число");
// int sum = Sun(number);
// Console.WriteLine($"Сумма цифр в числе = {sum}");


//-------------------------------------------------------------------------


// Напишите программу факториала 

// int Print (string Message)
// {
//     Console.WriteLine(Message);
//     string peremen1 = Console.ReadLine();
//     int peremen2 = Convert.ToInt32(peremen1);
//     return peremen2; 
// }

// int Factorial (int number)
// {
//     int y = 1;
//     int x = 0;
//     for (int i = 0; i<number; i++) 
//     {
//         x = x + 1;
//         y = y*x;
//     }
//     return y;
    
// }

// int X = Print("Введите число");
// int Y = Factorial(X);
// Console.WriteLine(Y);

// Написать программу которая выводит массив из 8 элементов созданный
// из случайных 0 и 1 

int Print (string Message)
{
    Console.WriteLine(Message);
    string peremen1 = Console.ReadLine();
    int peremen2 = Convert.ToInt32(peremen1);
    return peremen2; 
}

void Massiv (int [] number)
{
    int Dlina = number.Length;
    for (int i = 0; i<Dlina; i++)
    {
        number[i]= new Random().Next(0,2);
        Console.Write($"{number[i]} ");
    }
}
int x = Print("Введите число");
int [] array = new int[x];
Massiv (array);
