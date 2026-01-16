// int [] arr = {1,5,4,2,6,7,1,2};
// void PrintArray (int []array)
// {
//     int index = array.Length;
//     for (index = 0; index<array.Length; index++)
//     {
//         Console.Write($"{array[index]} ");
//     }
//     Console.WriteLine();
// }


// void Arr (int[] arrya)
// {
//     for (int count = 0; count<arrya.Length-1; count++)
//     {
//         int MinBol = count;
//         for (int j = count +1 ; j<arrya.Length; j++)
//         {
//             if ( arrya[j] < arrya[MinBol]) MinBol=j;
//         }
//     int swop = arrya[count];
//     arrya[count] = arrya[MinBol];
//     arrya[MinBol] = swop;
//     }
// }

// PrintArray(arr);
// Arr(arr);
// PrintArray(arr);


// Задача 25: Напишите цикл, который принимает на вход два числа (A и B) и возводит число A в натуральную степень B.
// 3, 5 -> 243 (3⁵)
// 2, 4 -> 16

// int Print (string number)
// {
//     Console.WriteLine(number); 
//     string peremen = Console.ReadLine();
//     int peremen2 =Convert.ToInt32(peremen);
//     return peremen2;

// }

// int NaturalDegree (int number, int degree)
// {
//     int peremen = 1;
//     for (int count = 0; count<degree; count++) 
//     {
//         peremen = peremen*number;
//         // Console.WriteLine(x3);
//     }
//     Console.WriteLine(peremen);
// return peremen;
// }

// int x1 = Print ("Введите число");
// int x2 = Print ("Введите число");
// NaturalDegree(x1,x2);



// Задача 27: Напишите программу, которая принимает на вход число и выдаёт сумму цифр в числе.
// 452 -> 11
// 82 -> 10
// 9012 -> 12


// int Print (string number)
// {
//     Console.WriteLine(number); 
//     string peremen = Console.ReadLine();
//     int peremen2 =Convert.ToInt32(peremen);
//     return peremen2;
// }
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
//     Console.WriteLine(x);
// }
// else
// {
//     Console.WriteLine ("Введите положительное число");
// }

// Задача 29: Напишите программу, которая задаёт массив из 8 элементов и выводит их на экран.
// 1, 2, 5, 7, 19 -> [1, 2, 5, 7, 19]
// 6, 1, 33 -> [6, 1, 33]

// int Print (string number)
// {
//     Console.WriteLine(number); 
//     string peremen = Console.ReadLine();
//     int peremen2 =Convert.ToInt32(peremen);
//     return peremen2;
// }

// void PrintMassiv (int [] peremen)
// {
//     int Dlina = peremen.Length;
//     for (int i = 0; i<Dlina; i++)
//     {
//         peremen[i]= new Random().Next(0,1000);
//         Console.Write($"{peremen[i]}  ");
//     }
// }

// int x = Print ("Введите длинну массива");
// int [] array = new int [x];
// PrintMassiv(array);
