using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BMO.GameDevUnity.CSharp1.Pract4
{
    static class StaticClass
    {
        public static int CountThreeNumber(int[] array)
        {
            int countThreeNumber = 0;
            for (int i = 1; i < array.Length; i++)
            {
                if (array[i] != 0 && array[i - 1] != 0)
                {
                    if (((array[i] % 3 == 0) ^ (array[i - 1] % 3 == 0)))
                    {
                        countThreeNumber++;
                    }
                }
            }
            return countThreeNumber;
        }

        public static void FillArray(out int[] array, int arrayLength)
        {
            array = new int[arrayLength];
            Random randomNumber = new Random();
            for (int i = 0; i < array.Length; i++)
            {
                array[i] = randomNumber.Next(-10000, 10000);
            }
        }

        public static int[] ArrayReadFile(string path)
        {
            int[] numberArray = new int[0];
            try
            {
                string stringFile = File.ReadAllText(path);
                string[] spliteFile = stringFile.Split(' ', '\n', '\r');
                Array.Resize(ref numberArray, spliteFile.Length);
                for (int i = 0; i < spliteFile.Length; i++)
                {
                    numberArray[i] = Convert.ToInt32(spliteFile[i]);
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Невозможно найти файл!");
            }
            catch (FormatException)
            {
                Console.WriteLine("Некорректные данные в файле!");
            }
            catch (Exception)
            {
                Console.WriteLine("Неопределенная ошибка");
            }
            return numberArray;
        }
    }
}