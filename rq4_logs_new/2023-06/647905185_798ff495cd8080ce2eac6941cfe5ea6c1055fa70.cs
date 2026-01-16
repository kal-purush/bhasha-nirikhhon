using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork_C_Sharp
{
#if false
    static void Main(string[] args)
    {
        int[] A = new int[5];
        double[,] B = new double[3, 4];
        Random random = new Random();

        // Заполнение одномерного массива A с клавиатуры
        Console.WriteLine("Введите 5 целых чисел для массива A:");
        for (int i = 0; i < A.Length; i++)
        {
            Console.Write("Введите число: ");
            A[i] = int.Parse(Console.ReadLine());
        }

        // Заполнение двумерного массива B случайными числами
        for (int i = 0; i < B.GetLength(0); i++)
        {
            for (int j = 0; j < B.GetLength(1); j++)
            {
                B[i, j] = random.NextDouble();
            }
        }

        Console.WriteLine("Массив A:");
        for (int i = 0; i < A.Length; i++)
        {
            Console.Write(A[i] + " ");
        }
        Console.WriteLine();

        Console.WriteLine("Массив B:");
        for (int i = 0; i < B.GetLength(0); i++)
        {
            for (int j = 0; j < B.GetLength(1); j++)
            {
                Console.Write(B[i, j] + " ");
            }
            Console.WriteLine();
        }

        int maxElement = A[0];
        int minElement = A[0];
        double sum = A[0];
        double product = A[0];

        for (int i = 1; i < A.Length; i++)
        {
            if (A[i] > maxElement)
            {
                maxElement = A[i];
            }

            if (A[i] < minElement)
            {
                minElement = A[i];
            }

            sum += A[i];
            product *= A[i];
        }

        for (int j = 0; j < B.GetLength(1); j++)
        {
            double columnSum = 0;
            for (int i = 0; i < B.GetLength(0); i++)
            {
                columnSum += B[i, j];
            }

            if (j % 2 != 0)
            {
                sum += columnSum;
            }
        }

        Console.WriteLine("Общий максимальный элемент: " + maxElement);
        Console.WriteLine("Общий минимальный элемент: " + minElement);
        Console.WriteLine("Общая сумма всех элементов: " + sum);
        Console.WriteLine("Общее произведение всех элементов: " + product);

        double evenSum = 0;
        for (int i = 0; i < A.Length; i++)
        {
            if (A[i] % 2 == 0)
            {
                evenSum += A[i];
            }
        }
        Console.WriteLine("Сумма четных элементов массива A: " + evenSum);
    } 
#endif

#if false
    static void Main(string[] args)
    {
        int[,] array = new int[5, 5];
        Random random = new Random();

        // Заполнение двумерного массива случайными числами
        for (int i = 0; i < array.GetLength(0); i++)
        {
            for (int j = 0; j < array.GetLength(1); j++)
            {
                array[i, j] = random.Next(-100, 101);
            }
        }

        // Вывод массива на экран
        Console.WriteLine("Массив:");
        for (int i = 0; i < array.GetLength(0); i++)
        {
            for (int j = 0; j < array.GetLength(1); j++)
            {
                Console.Write(array[i, j] + " ");
            }
            Console.WriteLine();
        }

        // Поиск минимального и максимального элементов
        int minElement = array[0, 0];
        int maxElement = array[0, 0];
        int minRow = 0, minColumn = 0, maxRow = 0, maxColumn = 0;

        for (int i = 0; i < array.GetLength(0); i++)
        {
            for (int j = 0; j < array.GetLength(1); j++)
            {
                if (array[i, j] < minElement)
                {
                    minElement = array[i, j];
                    minRow = i;
                    minColumn = j;
                }

                if (array[i, j] > maxElement)
                {
                    maxElement = array[i, j];
                    maxRow = i;
                    maxColumn = j;
                }
            }
        }

        // Определение суммы элементов между минимальным и максимальным элементами
        int sum = 0;
        int startRow = Math.Min(minRow, maxRow) + 1;
        int endRow = Math.Max(minRow, maxRow) - 1;
        int startColumn = Math.Min(minColumn, maxColumn) + 1;
        int endColumn = Math.Max(minColumn, maxColumn) - 1;

        for (int i = startRow; i <= endRow; i++)
        {
            for (int j = startColumn; j <= endColumn; j++)
            {
                sum += array[i, j];
            }
        }

        Console.WriteLine("Сумма элементов между минимальным и максимальным элементами: " + sum);
    } 
#endif


#if false
    static void Main(string[] args)
    {
        Console.Write("Введите строку для шифрования: ");
        string input = Console.ReadLine();

        Console.Write("Введите сдвиг: ");
        int shift = int.Parse(Console.ReadLine());

        string encryptedText = Encrypt(input, shift);
        Console.WriteLine("Зашифрованная строка: " + encryptedText);

        string decryptedText = Decrypt(encryptedText, shift);
        Console.WriteLine("Расшифрованная строка: " + decryptedText);
    }

    static string Encrypt(string input, int shift)
    {
        string encryptedText = "";

        foreach (char c in input)
        {
            if (char.IsLetter(c))
            {
                char encryptedChar = (char)(((c - 'A' + shift) % 26) + 'A');
                encryptedText += encryptedChar;
            }
            else
            {
                encryptedText += c;
            }
        }

        return encryptedText;
    }

    static string Decrypt(string input, int shift)
    {
        string decryptedText = "";

        foreach (char c in input)
        {
            if (char.IsLetter(c))
            {
                char decryptedChar = (char)(((c - 'A' - shift + 26) % 26) + 'A');
                decryptedText += decryptedChar;
            }
            else
            {
                decryptedText += c;
            }
        }

        return decryptedText;
    } 
#endif


}