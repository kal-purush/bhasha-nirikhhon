using System;
using System.IO;


namespace ConsoleApp23
{
    class Program
    {
        static void Sort(int[] a)//сортировка методом пузырька(можно воспользоваться интерфейсом IComparer)
        {
            int temp;
            for (int i = 0; i < a.Length - 1; ++i)
            {
                for (int j = a.Length - 1; j > i; --j)
                {
                    if (a[j] < a[j - 1])
                    {
                        temp = a[j];
                        a[j] = a[j - 1];
                        a[j - 1] = temp;
                    }
                }
            }
        }

        static bool Find(int[] a, int key)//функция для поиска, значени для поиска вводится в консоль
        {
            bool found = false;
            int i = 0;
            while (!found && i < a.Length)
            {
                if (a[i] == key)
                {
                    found = true;
                }
                else
                {
                    ++i;
                }
            }
            if (found)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        static void Main()
        {
            using (StreamReader fileIn = new StreamReader("fileIn.txt"))//последовательность целых чисел вводится в файл
            {
                using (StreamWriter fileOut = new StreamWriter("fileOut.txt"))
                {
                    string[] line = fileIn.ReadToEnd().Split();
                    int[] number = new int[line.Length];
                    for (int i = 0; i < line.Length; ++i)
                    {
                        number[i] = int.Parse(line[i]);
                    }
                    Console.Write("Введите значени для поиска: ");
                    int k = int.Parse(Console.ReadLine());
                    Sort(number);
                    fileOut.WriteLine("Отсортированный миссив:");
                    fileOut.WriteLine("Содержится ли число {0} в массиве: {1}", k, Find(number, k));
                    foreach (int elem in number)
                    {
                        fileOut.WriteLine("{0}", elem);
                    }
                }
            }
            
        }
    }
}