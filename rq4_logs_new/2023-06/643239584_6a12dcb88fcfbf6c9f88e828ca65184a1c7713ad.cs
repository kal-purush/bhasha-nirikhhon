// Напишите прошрамму, которая выведет все натуральный числа в промежутке от 
// 1 до N Применять метод рекурсии 
 

// int Imput (string messang)
// {
//     Console.Write(messang);
//     int peremen = Convert.ToInt32(Console.ReadLine());
//     return peremen;
// }
// int startNum = 1;
// int N = Imput ("Введите число > ");
// System.Console.WriteLine(PrintNumbers(startNum,N));
// string PrintNumbers (int num1, int num2)
// {
//     if (num1==num2)
//     {
//         return num1.ToString();
//     }
//     return (num1+" "+ PrintNumbers(num1+1,num2));
// }

//-----------------------------------------------------------------------------------
// Напишите программу которая выведет все натуральный числа от N до M 

// int Imput (string messang)
// {
//     Console.Write(messang);
//     int peremen = Convert.ToInt32(Console.ReadLine());
//     return peremen;
// }
// int M = Imput ("Введите начальное число M > ");
// int N = Imput ("Введите конечное число N > ");

// string PrintNumbers (int num1, int num2)
// {
//     if (M<N)
//     {

//        if (num1==num2)
//         {
//             return num1.ToString();
//         }
//         return (num1+" "+ PrintNumbers(num1+1,num2));        
//     } 
//     else 
//     {
//       return ($"Начальное число ''{M}'' больше конечного ''{N}'' ");
//     } 
        
// }

// System.Console.WriteLine(PrintNumbers(M,N));


//------------------------------------------------------------------------------------------

// Напишите программу которая будет принимать число и выдавать его сумму чифр спомощью рекурсии 


// int Imput (string messang)
// {
//     Console.Write(messang);
//     int peremen = Convert.ToInt32(Console.ReadLine());
//     return peremen;
// }
// int M = Imput ("Введите начальное число M > ");
// int Sum (int m)
// {
//     if (m==0)
//     {
//         return 0;
//     }
//     return (m%10+Sum(m/10));
// }

// System.Console.WriteLine(Sum(M));


//----------------------------------------------------------------------------------

//Напишите программу которая запрашивает два числа и возводит первое число в кварат второго спомощью рекурсии


int Imput (string messang)
{
    Console.Write(messang);
    int peremen = Convert.ToInt32(Console.ReadLine());
    return peremen;
}
int M = Imput ("Введите число > ");
int N = Imput ("Введите число в квадрат которого хотите возвести > ");

int Square (int m, int n)
{
    if (n==0) return 1;

    if(n == 1) return m;
    
    return (m*Square(m, n-1));
}

System.Console.WriteLine(Square(M,N));