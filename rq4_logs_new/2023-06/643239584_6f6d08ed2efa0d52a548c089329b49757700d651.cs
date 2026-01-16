// Задача 25: Напишите цикл, который принимает на вход два числа (A и B) и возводит число A в натуральную степень B.
// 3, 5 -> 243 (3⁵)
// 2, 4 -> 16

int Print (string number)
{
    Console.WriteLine(number); 
    string peremen = Console.ReadLine();
    int peremen2 =Convert.ToInt32(peremen);
    return peremen2;

}
// double NaturalDegree (int number, int degree)
// {
// if ((degree>0)&&(number>0))
// {
//     int peremen = 1;
//     for (int count = 0; count<degree; count++) 
//     {
//         peremen = peremen*number;
//     }
//     return peremen;
// }

// else if ((degree<0)&&(number<0))
// {
//     double peremen1 = 1;
//     for (int count = 0; count>degree; count--) 
//     {
//         peremen1 = peremen1*number;
//     }
//             double d = 1/peremen1;
//             return d;
//     }
// else if ((degree<0)&&(number>0))
//     {
//     double peremen2 = 1;
//     for (int count = 0; count>degree; count--) 
//     {
//         peremen2 = peremen2*number;
//     }
//     double d = 1/peremen2;
//     return d;
//     }
// else 
// {
//     int peremen4 = 1;
//     for (int count = 0; count<degree; count++) 
//     {
//         peremen4 = peremen4*number;
//     }
//         return peremen4;
    
// }
// }

// int x1 = Print ("Введите число");
// int x2 = Print ("Введите степень");
// double X = NaturalDegree(x1,x2);
// Console.WriteLine($"Степень числа = {X}");



//------------------------------------------------------------------------------------------



// Задача 27: Напишите программу, которая принимает на вход число и выдаёт сумму цифр в числе.
// 452 -> 11
// 82 -> 10
// 9012 -> 12



// int n = Print ("Введите число");
// if (n>=0)
// {
//     int x = 0;
//     while(n > 0)
//     {
//         int q = (n % 10);
//         n = (n / 10);
//         x = x + q;
//     }
//     Console.WriteLine($"Сумма цифр числа = {x}");
// }
// else
// {
//     n = n * (-1);
//     int x = 0;
//     while(n > 0)
//     {
//         int q = (n % 10);
//         n = (n / 10);
//         x = x + q;
//     }
//     Console.WriteLine($"Сумма цифр числа = {x}");
// }



//------------------------------------------------------------------------------------------

// Задача 29: Напишите программу, которая задаёт массив из 8 элементов и выводит их на экран.
// 1, 2, 5, 7, 19 -> [1, 2, 5, 7, 19]
// 6, 1, 33 -> [6, 1, 33]


// void PrintMassiv (int [] peremen)
// {
//     int Dlina = peremen.Length;
//     for (int i = 0; i<Dlina; i++)
//     {
//         peremen[i]= new Random().Next(-1000,1000);
//         Console.Write($"{peremen[i]}  ");
//     }
// }

// int x = Print ("Введите длинну массива");
// int [] array = new int [x];
// PrintMassiv(array);