using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Home_3
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Task0();
            Task1();
            Task2();
            Task3();
            Task4();
            Task5();
            Task6();
            Task7();
            Task8();
            Task9();

            Console.ReadLine();
        }


        /*
        9. доп. Создайте массив из n случайных целых чисел и выведите его на экран.  
        Размер массива пусть задается с консоли и размер массива может быть больше 5 и меньше или равно 10.  
        Если n не удовлетворяет условию - выведите сообщение об этом.  Если пользователь ввёл не подходящее число, то программа должна просить пользователя повторить ввод.  
        Создайте второй массив только из чётных элементов первого массива, если они там есть, и вывести его на экран.
        */
        private static void Task9()
        {
            int number = 1;
            do
            {
                Console.WriteLine("input array range:\n");
                bool numberCheck = int.TryParse(Console.ReadLine(), out number);
                if (numberCheck == false)
                {
                    Console.WriteLine("was entered incorrected operand type, range could be a number, Try another ");
                    return;
                }
                else if (number <= 0)
                {
                    Console.WriteLine("Number must be possitive and not 0");
                }
                else if (number < 5 || number > 10)
                {
                    Console.WriteLine("Number must be more than 5 and less or equal the 10. Try another");
                }
                else break;
            }
            while (true);

            Console.WriteLine("\narray range was inputed \n");

            Random random = new Random();
            int[] array1 = new int[number];
            int[] array2 = { };
            Console.WriteLine("Generating array:\n");

            for (int i = 0, j = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");

                if (array1[i] % 2 == 0)
                {
                    Array.Resize(ref array2, j + 1);
                    array2[j] = array1[i];
                    j++;
                }

            }
            Console.WriteLine();

            Console.WriteLine("New array:\n");

            for (int i = 0; i < array2.Length; i++)
            {
                Console.Write(array2[i] + " ");
            }


        }


        /*  8 Доп. Создайте двумерный массив. Выведите на консоль диагонали массива.*/
        private static void Task8()
        {
            int sumn = 0;
            Random random = new Random();

            Console.WriteLine("input array level:\n");
            bool numberCheck = int.TryParse(Console.ReadLine(), out int number);
            if (numberCheck == false)
            {
                Console.WriteLine("was entered incorrected operand type, level could be a number");
                return;
            }
            if (number <= 0)
            {
                Console.WriteLine("Number must be possitive and not 0");
            }

            int[,] matrix1 = new int[number, number];
            int[] matrix2 = new int[number];
            int[] matrix3 = new int[number];

            Console.WriteLine("generated array:\n");
            for (int i = 0; i < matrix1.GetLength(0); i++)
            {
                for (int j = 0; j < matrix1.GetLength(1); j++)
                {
                    matrix1[i, j] = random.Next(1, 50);
                    Console.Write(matrix1[i, j] + " ");
                }
                Console.WriteLine();
            }

            Console.WriteLine("\ndiagonal 1: \n");
            for (int j = 0; j < matrix1.GetLength(0); j++)
            {
                matrix2[j] = matrix1[j, j];
                Console.Write(matrix2[j] + " ");
            }

            Console.WriteLine("\ndiagonal 2: \n");
            for (int j = 0; j < matrix1.GetLength(0); j++)
            {
                matrix3[j] = matrix1[number - 1 - j, j];
                Console.Write(matrix3[j] + " ");
            }
        }


        /*  7. Создайте двумерный массив целых чисел. Выведите на консоль сумму всех
элементов массива. */
        private static void Task7()
        {
            int sumn = 0;
            Random random = new Random();

            Console.WriteLine("input number of rows and the range of array:\n");
            int[,] matrix2 = new int[int.Parse(Console.ReadLine()), int.Parse(Console.ReadLine())];

            Console.WriteLine("generated array:\n");
            for (int i = 0; i < matrix2.GetLength(0); i++)
            {
                for (int j = 0; j < matrix2.GetLength(1); j++)
                {
                    matrix2[i, j] = random.Next(1, 50);
                    Console.Write(matrix2[i, j] + " ");
                    sumn += matrix2[i, j];
                }
                Console.WriteLine();
            }
            Console.WriteLine("\nSum all elements of array: " + sumn);
        }


        /* 6.Реализуйте алгоритм сортировки массива пузырьком */
        private static void Task6()
        {
            Random random = new Random();

            Console.WriteLine("input array range:\n");
            int[] array1 = new int[int.Parse(Console.ReadLine())];

            Console.WriteLine("Generating array:\n");
            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");
            }
            Console.WriteLine();

            //bubble sorting
            for (int i = 0; i < array1.Length - 1; i++)
            {
                for (int j = 0; j < array1.Length - i - 1; j++)
                {
                    if (array1[j] > array1[j + 1])
                    {
                        int oper = array1[j];
                        array1[j] = array1[j + 1];
                        array1[j + 1] = oper;
                    }

                }
            }

            Console.WriteLine("\n sorting array bubble method :\n");
            for (int i = 0; i < array1.Length; i++)
            {
                Console.Write(array1[i] + " ");
            }
            Console.WriteLine();

        }


        /*5. Создайте массив строк. Заполните его произвольными именами людей.  
       Отсортируйте массив.  
       Результат выведите на консоль.  

       */
        private static void Task5()
        {
            string[] array1 =
            {
                "John","Roy","Dj","BoBa","12Ninja","Alla","3","2","Петя","Aлла"
            };

            Console.WriteLine("Names Array: ");
            for (int i = 0; i < array1.Length; i++)
            {
                Console.Write(array1[i] + " ");
            }

            Array.Sort(array1);

            Console.WriteLine("\nSorted Array:\n");
            for (int i = 0; i < array1.Length; i++)
            {
                Console.Write(array1[i] + ", ");
            }

        }


        /* 4. Создайте массив и заполните массив.  
        Выведите массив на экран в строку.  
        Замените каждый элемент с нечётным индексом на ноль.  
        Снова выведете массив на экран на отдельной строке.  */
        private static void Task4()
        {
            Random random = new Random();

            Console.WriteLine("input array range:\n");
            int[] array1 = new int[int.Parse(Console.ReadLine())];

            Console.WriteLine("Generating array:\n");
            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");
            }
            Console.WriteLine();

            Console.WriteLine("\nmodificated array:\n");
            for (int i = 0; i < array1.Length; i++)
            {
                if (array1[i] % 2 != 0)
                {
                    array1[i] = 0;
                }
                Console.Write(array1[i] + " ");
            }
        }


        /*3. Создайте 2 массива из 5 чисел.  
        Выведите массивы на консоль в двух отдельных строках.  
        Посчитайте среднее арифметическое элементов каждого массива и сообщите, для какого из массивов это значение оказалось больше
        (либо сообщите, что их средние арифметические равны).   */
        private static void Task3()
        {
            double res1 = 0, res2 = 0;
            Random random = new Random();

            Console.WriteLine("input array range1:\n");
            int[] array1 = new int[int.Parse(Console.ReadLine())];

            Console.WriteLine("input array range2:\n");
            int[] array2 = new int[int.Parse(Console.ReadLine())];

            Console.WriteLine("Generating massive 1:\n");
            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");
                res1 += array1[i];
            }
            Console.WriteLine();

            Console.WriteLine("Generating array 2:\n");
            for (int i = 0; i < array2.Length; i++)
            {
                array2[i] = random.Next(1, 100);
                Console.Write(array2[i] + " ");
                res2 += array2[i];
            }
            Console.WriteLine();

            if (res1 / array1.Length == res2 / array2.Length)
            {
                Console.WriteLine("Avarage sum arrays are equal");
            }
            if (res1 / array1.Length > res2 / array2.Length)
            {
                Console.WriteLine("Avarage sum array1 more than avarage sum array 2");
            }
            if (res1 / array1.Length < res2 / array2.Length)
            {
                Console.WriteLine("Avarage sum array2 more than avarage sum array 1");
            }
            Console.WriteLine("Avarage sum array 1: " + res1 / array1.Length + "\nAvvarage sum array 2: " + res2 / array2.Length);
        }


        /*  2. Создайте и заполните массив случайным числами и выведете максимальное, минимальное и среднее значение.  
        Для генерации случайного числа используйте метод Random() .  Пусть будет возможность создавать массив произвольного размера.
       Пусть размер массива вводится с консоли.  */
        private static void Task2()
        {
            Random random = new Random();

            Console.WriteLine("input massive range:\n");
            int[] array1 = new int[int.Parse(Console.ReadLine())];

            Console.WriteLine("Generating massive:\n");
            for (int i = 0; i < array1.Length; i++)
            {
                array1[i] = random.Next(1, 100);
                Console.Write(array1[i] + " ");
            }
            Console.WriteLine();

            int maxop = array1[0], minop = array1[0], result = 0;

            for (int i = 0; i < array1.Length; i++)
            {
                if (array1[i] < minop)
                {
                    minop = array1[i];
                }
                if (array1[i] > maxop)
                {
                    maxop = array1[i];
                }
                result += array1[i];
            }

            Console.WriteLine("Maxium: " + maxop);
            Console.WriteLine("Minium: " + minop);
            Console.WriteLine("Avarage sum elements: " + result / array1.Length);
        }

        /*   1. Создайте массив целых чисел. Удалите все вхождения заданного числа из массива.  
        Пусть число задается с консоли. Если такого числа нет - выведите сообщения об этом.  
        В результате должен быть новый массив без указанного числа.  */
        private static void Task1()
        {

            int[] matrix = { 1, 2, 3, 2, 5, 6, 2, 8, 9, 2, 11, 2 };
            int[] matrix2 = { };
            int j = 0;

            Console.WriteLine("input number for delete from array");
            bool numberCheck = double.TryParse(Console.ReadLine().Replace(',', '.'), out double operand1);

            if (numberCheck == false)
            {
                Console.WriteLine("incorrected operand type was entered");
                return;
            }
            if (operand1 % 1 != 0)
            {
                Console.WriteLine("Number must be integer");
                return;
            }


            for (int i = 0; i < matrix.GetLength(0); i++)
            {
                if (matrix[i] == operand1)
                {
                    continue;
                }
                Array.Resize(ref matrix2, j + 1);
                matrix2[j] = matrix[i];
                j++;
            }

            Console.WriteLine("new Array");
            for (int i = 0; i < matrix2.GetLength(0); i++)
            {
                Console.Write(matrix2[i] + " ");
            }
            Console.WriteLine();

            Console.WriteLine("old Array");
            for (int i = 0; i < matrix.GetLength(0); i++)
            {
                Console.Write(matrix[i] + " ");
            }
            Console.WriteLine();
        }

        /*  0.Создайте массив целых чисел. Напишете программу, которая выводит сообщение о том, входит ли заданное число в массив или нет. 
      Пусть число для поиска задается с консоли.*/
        private static void Task0()
        {
            int[,] matrix =
            {
                {1,2,3},
                {4,5,6},
                {7,8,9},
                {10,11,12}
             };
            bool b = false;

            Console.WriteLine("input number for check him to belong the array");
            bool numberCheck = double.TryParse(Console.ReadLine().Replace(',', '.'), out double operand1);

            if (numberCheck == false)
            {
                Console.WriteLine("was entered incorrected operand type");
                return;
            }
            if (operand1 % 1 != 0)
            {
                Console.WriteLine("Number must be integer");
                return;
            }

            for (int i = 0; i < matrix.GetLength(0); i++)
            {
                for (int j = 0; j < matrix.GetLength(1); j++)
                {
                    if (matrix[i, j] == operand1)
                    { b = true; break; }
                }
            }

            if (b == false)
            {
                Console.WriteLine("The number " + operand1 + " don't belong the array");
            }
            else
            {
                Console.WriteLine("The number " + operand1 + " to belong the array");
            }

            for (int i = 0; i < matrix.GetLength(0); i++)
            {
                for (int j = 0; j < matrix.GetLength(1); j++)
                {
                    Console.Write(matrix[i, j] + " ");
                }
                Console.WriteLine();
            }
        }

    }
}