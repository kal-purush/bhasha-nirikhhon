using System;
using UtilityLibraries;

namespace GB1Lesson4
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //C:\Users\{user}\Downloads\two-dim-3x5.csv
            string PathToFile = ConsoleHelper.ReadInputUntillCorrect("Enter path to csv file (seprator must be ',')", ConsoleHelper.ReadAnyNonEmptyString);

            uint AnyInt = ConsoleHelper.ReadInputUntillCorrect("Enter any positive int", ConsoleHelper.ReadAnyPositiveInteger);

            //C:\Users\{user}\Downloads\
            string Directory = ConsoleHelper.ReadInputUntillCorrect("Enter path to output directory", ConsoleHelper.ReadAnyNonEmptyString);

            TwoDimentionalArray TwoDimArray = new TwoDimentionalArray(PathToFile);
            Console.WriteLine(TwoDimArray.ToString());

            Console.WriteLine($"Максимальный элемент: {TwoDimArray.MaxElement}");
            Console.WriteLine($"Минимальный элемент: {TwoDimArray.MinElement}");

            TwoDimArray.Sum(out int sum);
            Console.WriteLine($"Сумма всех значений: {sum}");

            TwoDimArray.SumElementsGreaterThanProvided((int)AnyInt, out int sumFromMax);
            Console.WriteLine($"Сумма всех значений, начиная с {AnyInt}: {sumFromMax}");

            TwoDimArray.GetMaxElementIndex(out int row, out int column);
            Console.WriteLine($"Индекс максимального элемента: [{row}, {column}]");

            Console.WriteLine($"Данные выгружены в: [{TwoDimArray.OtputdataToDirectory(Directory)}]");

            Console.ReadKey();
        }
    }
}