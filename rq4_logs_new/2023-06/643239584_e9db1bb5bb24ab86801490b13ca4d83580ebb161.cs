// // Задача 40: Напишите программу, которая принимает на вход три числа и проверяет,
// //  может ли существовать треугольник с сторонами такой длины.

// int Imput (string messang)
// {
//     Console.WriteLine(messang);
//     string peremen1 = Console.ReadLine();
//     int peremen2 = Convert.ToInt32(peremen1);
//     return peremen2;
// }

// void Treugol (int x, int y, int z)
// {
//     if ((z<x+y)&&(x<y+z)&&(y<x+z))
//     {
//         Console.WriteLine("Треугольник существует");
//     }
//     else
//     {
//         Console.WriteLine("Треугольник не существует");
//     }
// }

// int x = Imput("Введите число");
// int y = Imput("Введите число");
// int z = Imput("Введите число");
// Treugol(x,y,z);

//-------------------------------------------------------------------------------

// int Imput (string messang)
// {
//     Console.WriteLine(messang);
//     string peremen1 = Console.ReadLine();
//     int peremen2 = Convert.ToInt32(peremen1);
//     return peremen2;
// }

// string XX (int number)
// {
//     string result= "";
//     while(number>0)
//     {
//         result = number%2+result;
//         number = number/2;
//     }
//     Console.WriteLine(result);
//     return result;
// }

// int x = Imput ("Введите число");
// XX(x);

//-------------------------------------------------------------------------

// Задача 44: Не используя рекурсию, выведите первые N чисел Фибоначчи. Первые два числа Фибоначчи: 0 и 1.
// Если N = 5 -> 0 1 1 2 3
// Если N = 3 -> 0 1 1
// Если N = 7 -> 0 1 1 2 3 5 8

// int Imput (string messang)
// {
//     Console.WriteLine(messang);
//     string peremen1 = Console.ReadLine();
//     int peremen2 = Convert.ToInt32(peremen1);
//     return peremen2;
// }

// void Fibonachi (int x)
// {
//     int firstNun = 0;
//     int secondNum = 1;
//     Console.WriteLine($"{1} = {firstNun}");
//     Console.WriteLine($"{2} = {secondNum}");
//     for (int i = 3; i<=x; i++)
//     {
//         int newNum = firstNun+secondNum;
//         Console.WriteLine($"{i} = {newNum}");

//         firstNun = secondNum;
//         secondNum = newNum;

//     }
// }

// int x = Imput("Введите число");
// Fibonachi (x);

//---------------------------------------------------------------------------------

// Задача 39: Напишите программу, которая перевернёт одномерный массив (последний элемент будет на первом месте, а первый - на последнем и т.д.)
// [1 2 3 4 5] -> [5 4 3 2 1]
// [6 7 3 6] -> [6 3 7 6]


int [] array = new int [] {1,2,3,4,5};

for (int i = 0; i < array.Length/2; i++)
{
    int temp = array[i];
    array[i]=array[array.Length-1-i];
    array[array.Length-1-i]=temp;
}
// for (int i = 0; i < array.Length; i++)
// {
//     Console.Write(array[i]+" ");
// }

// Вместо цикла вывода массива можно использовать встроеннную функцию
// String.Join 

Console.WriteLine(String.Join("|",array));
