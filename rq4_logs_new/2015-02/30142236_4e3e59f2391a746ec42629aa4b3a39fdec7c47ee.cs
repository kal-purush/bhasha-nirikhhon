using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Anagramms
{
    /// <summary>
    /// Достал я словарь на 40 Мб примерно. 50 раз перезаписал его содержимое - словарь стал 2 Гб,
    /// на нем проверялась работоспособность приведенного ниже метода.
    /// Загрузил только это т.к. метод подходит для обоих задач
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine("Введите путь к файлу: ");
            string path = Console.ReadLine();

            try
            {
                FileInfo flinfo = new FileInfo(path);

                while (!flinfo.Exists)
                {
                    Console.WriteLine("Введите путь заново: ");

                    path = Console.ReadLine();
                    flinfo = new FileInfo(path);
                }

                Console.WriteLine("Ждите...");

                #region Main Logic
                foreach (Int32 sizeOfWords in Methods.CreateObject.ReadInput(path).Select(s => s.Length).Distinct().OrderBy(s => s))
                {
                    var fromDictionary = File.ReadLines(path).Distinct().Where(s => s.Length == sizeOfWords);
                    var groups1 = from string words in fromDictionary
                                  group words by string.Concat(words.OrderBy(x => x)) into cwords
                                  group cwords by cwords.Count() into d
                                  orderby d.Key descending
                                  select d;
                    foreach (var arrayOfWords in groups1.First())
                    {
                        if (arrayOfWords.Count() != 1)
                        {
                            Console.WriteLine(string.Join(", ", arrayOfWords));

                        }
                    }
                }
                #endregion

                Console.ForegroundColor = ConsoleColor.Red; Console.WriteLine("Нажмите клавишу для выхода...");
                Console.ReadKey();
            }
            catch (Exception exp)
            { Console.WriteLine(exp.ToString()); }
            finally
            {
                Console.ReadKey();
            }
        }
    }

    public class Methods
    {
        protected Methods()
        { }

        private static Methods mtds = new Methods();
        public static Methods CreateObject
        { get { return mtds; } }

        public IEnumerable<string> ReadInput(string path)
        {
            using (var file = File.OpenText(path))
            {
                while (!file.EndOfStream)
                {
                    yield return file.ReadLine();
                }
            }
        }
    }
}