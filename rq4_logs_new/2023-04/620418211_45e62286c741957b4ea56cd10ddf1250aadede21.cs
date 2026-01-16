using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;

namespace Home_4
{
    internal class Program
    {
        static void Main(string[] args)
        {

            //Task1();
            //Task2();
            // Task3();
            // Task4();
            //Task5();
            Task6();

            Console.ReadLine();

        }

        /// <summary>
        /// 1. Задать строку содержащую внутри цифры и несколько повторений слова test, 
        /// Заменить в строке все вхождения 'test' на 'testing'.
        /// </summary>
        private static void Task1()
        {
            string taskString = "TesT1test22344TeST543 tEst 4345TeST";
            taskString = taskString.ToLower();

            string resultString = taskString.Replace("test", "testing");
            resultString = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(resultString);
            Console.WriteLine(resultString);
        }


        /// <summary>
        /// 2. Создайте переменные, которые будут хранить следующие слова: (Welcome, to, the, TMS, lessons)
        /// выполните конкатенацию слов и выведите на экран следующую фразу: Welcome to the TMS lessons. 
        ///Каждое слово должно быть записано отдельно и взято в кавычки, например
        ///"Welcome". Не забывайте о пробелах после каждого слова
        /// </summary>
        private static void Task2()
        {
            string taskString1 = "Welcome";
            string taskString2 = "to";
            string taskString3 = "the";
            string taskString4 = "TMS";
            string taskString5 = "lessons";

            string result = string.Join(" ", taskString1, taskString2, taskString3, taskString4, taskString5);
            Console.WriteLine(result);

        }


        /// <summary>
        /// Дана строка: teamwithsomeofexcersicesabcwanttomakeitbetter.
        /// Необходимо найти в данной строке "abc", 
        /// записав всё что до этих символов в переменную firstPart,
        ///а также всё, что после них во вторую secondPart.
        ///  Результат вывести в консоль.
        /// </summary>
        private static void Task3()
        {
            string taskString1 = "teamwithsomeofexcersicesabcwanttomakeitbetter";
            Console.WriteLine(taskString1);

            taskString1 = taskString1.Replace("abc", "*");
            string[] arr = taskString1.Split('*');
            string firstPart = arr[0];
            string secondPart = arr[1];

            Console.WriteLine("first part: " + firstPart);
            Console.WriteLine("Second part: " + secondPart);
        }


        /// <summary>
        /// 4. Дана строка: Good day 
        ///Необходимо с помощью метода substring удалить слово "Good". 
        ///После чего необходимо используя команду insert создать строку со значением: The best day!!!!!!!!!.
        ///Заменить последний "!" на "?" и вывести результат на консоль.
        /// </summary>
        private static void Task4()
        {
            string taskString1 = "Good day";
            int index = taskString1.LastIndexOf("day");
            Console.WriteLine(taskString1);
            Console.WriteLine(index);

            taskString1 = taskString1.Substring(index, taskString1.Length - index);
            Console.WriteLine(taskString1);

            taskString1 = taskString1.Insert(0, " The best");
            taskString1 = taskString1.Insert(taskString1.Length, "!!!!!!!!!");
            Console.WriteLine(taskString1);

            taskString1 = taskString1.Substring(0, taskString1.LastIndexOf("!"));
            taskString1 += "?";
            Console.WriteLine(taskString1);
        }



        /// <summary>
        /// 5. Введите с консоли строку, которая будет содержать буквы и цифры.
        /// Удалите из исходной строки все цифры и выведите их на экран.(Использовать метод Char.IsDigit(),
        /// его не разбирали на уроке, посмотрите, пожалуйста, документацию этого метода самостоятельно)
        /// </summary>
        private static void Task5()
        {
            Console.WriteLine("Input string with digit");
            string taskStr = Console.ReadLine();

            string taskStr2 = string.Empty;
            char[] chars = taskStr.ToCharArray();

            for (int i = 0; i < chars.Length; i++)
            {
                if (Char.IsDigit(chars[i]) == true)
                {
                    taskStr2 += chars[i];
                }
            }
            Console.WriteLine(taskStr2);
        }


        /// <summary>
        /// 6. Задайте 2 предложения из консоли.
        /// Для каждого слова первого предложения определите количество его вхождений во второе предложение.
        /// </summary>
        private static void Task6()
        {
            Console.WriteLine("Input string1 with spaces");
            string taskStr1 = Console.ReadLine();

            string[] arrStr = taskStr1.Split(' ');

            Console.WriteLine("Input string1 with spaces");
            string taskStr2 = Console.ReadLine();
            string[] arrStr2 = taskStr2.Split(' ');
            int[] arrindex = new int[arrStr.Length];


            for (int i = 0; i < arrStr.Length; i++)
            {
                for (int j = 0; j < arrStr2.Length; j++)
                {
                    if (string.Equals(arrStr[i], arrStr2[j])==true)
                    { 
                        arrindex[i]++;
                    }
                }
                Console.WriteLine("Word " + arrStr[i] + " contains in second  string " + arrindex[i] + " times");
            }

        }
    }
}