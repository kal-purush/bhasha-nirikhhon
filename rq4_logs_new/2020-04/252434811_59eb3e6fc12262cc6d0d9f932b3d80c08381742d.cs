using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    class DZ8
    {
        public static void Start()
        {
            string str = "Словосочетание";

            MyString myStr1 = new MyString(str);

            string substring = "ние";
            Console.WriteLine("Применение метода Contains");
            Console.WriteLine($"Строка: {str}" + (myStr1.Contains(substring) ? "" : " не" ) + $" содержит строку: {substring}\n");

            Console.WriteLine("Применение метода IndexOf");
            char ch = 'о';
            int index = myStr1.IndexOf(ch);
            if (index >= 0)
                Console.WriteLine($"В строке: {myStr1} первый найденый индекс символа {ch}: {index}\n");
            else
                Console.WriteLine($"В строке: {myStr1} символ {ch} не найден\n");

            
            Console.WriteLine("Применение индексатора класса");
            Console.WriteLine($"Получение символа строки по индексу:\n по индексу: {index} возвращается символ: {myStr1[index]}\n");

            Console.WriteLine("Применение автосвойства");
            Console.WriteLine($"Длина строки: {myStr1} - {myStr1.Length}\n");

            Console.WriteLine("Применение переопределение опреатора +");
            string str2 = "Простое";
            MyString myStr2 = new MyString(str2);
            Console.WriteLine($"Результат сложения двух классов MyString {myStr1} и {myStr2} будет: {myStr1 + myStr2}\n");



            //1.1 РАСШИРЕНИЯ********************************************************** 
            Console.WriteLine("Расширения класса строки");

            Console.WriteLine("Выбираем количество первых символов слева");
            Console.WriteLine(str);
            Console.WriteLine(str.CutLeft(12) + "\n");
            //1.2
            Console.WriteLine("Выбираем количество первых символов слева, исключая выбранный смивол");
            Console.WriteLine(str);
            Console.WriteLine(str.CutLeft(12, 'о') + "\n");
            //1.3
            Console.WriteLine("Выбираем количество первых символов слева, исключая массив смиволов");
            Console.WriteLine(str);
            Console.WriteLine(str.CutLeft(12, new char[] { 'о', 'с' }) + "\n");

            //1.4***********************************************************
            Console.WriteLine("Выбираем количество первых символов справа");
            Console.WriteLine(str);
            Console.WriteLine(str.CutRight(12) + "\n");
            //1.5
            Console.WriteLine("Выбираем количество первых символов справа, исключая выбранный смивол");
            Console.WriteLine(str);
            Console.WriteLine(str.CutRight(12, 'о') + "\n");
            //1.6
            Console.WriteLine("Выбираем количество первых символов справа, исключая массив смиволов");
            Console.WriteLine(str);
            Console.WriteLine(str.CutRight(12, new char[] { 'о', 'с' }) + "\n");

        }
    }

    public class MyString
    {
        char[] myCharArray;

        public int Length
        {
            get
            {
                return myCharArray.Length;
            }
        }

        public override string ToString()
        {
            string str = "";
            foreach (var item in myCharArray)
            {
                str += item;
            }

            return str;
        }

        public static MyString operator +(MyString str1, MyString str2)
        {
            string str3 = str1.ToString() + str2.ToString();

            return new MyString(str3);
        }
            

        private MyString()
        {

        }

        public MyString(char[] charArray)
        {
            myCharArray = new Char[charArray.Length - 1];

            for (int i = 0; i < charArray.Length; i++)
            {
                myCharArray[i] = charArray[i];
            }
        }

        public MyString(string str)
        {
            myCharArray = new Char[str.Length];

            for (int i = 0; i < str.Length; i++)
            {
                myCharArray[i] = str[i];
            }
        }

        public bool Contains(string str)
        {
            //если подстрока длиннее исходной строки
            if (str.Length > myCharArray.Length)
            {
                return false;
            }

            for (int i = 0; i < myCharArray.Length; i++)
            {
                //если подстрока длинее чем остаток начальной строки
                if (myCharArray.Length - i - str.Length  < 0) return false;

                int nextIndex = 0;
                while (nextIndex < str.Length
                    && myCharArray[i + nextIndex] == str[nextIndex])
                {
                    nextIndex++;
                }
                if (nextIndex == str.Length)
                    return true;
            }

            return false;
        }

        public int IndexOf(char ch)
        {
            int Index = -1;

            for (int i = 0; i < myCharArray.Length; i++)
            {
                if (myCharArray[i] == ch)
                {
                    Index = i;
                    break;
                }
            }

            return Index;
        }

        public char this[int index]
        {
            get
            {
                if (index < myCharArray.Length)
                {
                    return myCharArray[index];
                }
                else return new char();
            }

        }
    }
}