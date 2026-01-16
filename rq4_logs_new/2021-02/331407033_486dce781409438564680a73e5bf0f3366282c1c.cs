using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace lesson_5
{
    
    public static class StringsAndChars
    {
        #region Task1

        /*
         * 1. Создать программу, которая будет проверять
         * корректность ввода логина. Корректным логином будет строка от 2 до 10 символов,
         * содержащая только буквы латинского алфавита или цифры, при этом цифра не может быть первой:
         * а) без использования регулярных выражений;
         * б) с использованием регулярных выражений.
         * Павлов Алексей
         */
        private static bool CheckLogin(string login)
        {
            return (login.Length >= 2) && (login.Length <= 10) && (!char.IsDigit(login[0])) &&
                   login.All(c => c > 'A' && c < 'z');
        }

        private static bool CheckLoginRegx(string login)
        {
            var regex = new Regex(@"^[A-Za-z]{1,}\w{1,9}$");
            return regex.IsMatch(login);
        }

        private static void Task1()
        {
            Console.WriteLine("Введите логин длинной от 2 до 10 символов, только буквы латинского алфавита и цифры");
            var input = Console.ReadLine();
            Console.WriteLine($"без регулярки {CheckLogin(input)} с регуляркой {CheckLoginRegx(input)}");
        }

        #endregion

        #region Task2

        /*
        * 2. Разработать класс Message, содержащий следующие статические методы для обработки
        * текста:
        * а) Вывести только те слова сообщения, которые содержат не более n букв.
        * б) Удалить из сообщения все слова, которые заканчиваются на заданный символ.
        * в) Найти самое длинное слово сообщения.
        * г) Сформировать строку с помощью StringBuilder из самых длинных слов сообщения.
        * Продемонстрируйте работу программы на текстовом файле с вашей программой.
        * Павлов Алексей
        */

        private static class Message
        {
            public static void LessThenNWords(int n, string[] messages)
            {
                foreach (var msg in messages)
                {
                    if (Regex.Replace(msg, @"([^A-Za-z])|(\s+)", "")
                        .Length <= n)
                        Console.WriteLine(msg);
                }
            }

            public static void ExeptChEnding(char ch, string[] messages)
            {
                foreach (var msg in messages)
                {
                    var words = msg.Split(' ');
                    var msgToPrint = msg;
                    foreach (var word in words)
                    {
                        if (word[word.Length - 1].Equals(ch) && (msg.Contains(word)))
                            msgToPrint = msg.Replace(word, "");
                    }

                    Console.WriteLine(Regex.Replace(msgToPrint, @"\s+", " ").Trim());
                }
            }

            private static string LongestWordInMsg(string message)
            {
                var longestWord = string.Empty;
                foreach (var word in message.Split(' '))
                {
                    if (longestWord.Length < word.Length)
                        longestWord = word;
                }

                return longestWord;
            }

            public static void MessageOfLongestWord(string[] messages)
            {
                var message = new StringBuilder(string.Empty);
                foreach (var msg in messages)
                {
                    message.Append($"{Message.LongestWordInMsg(msg)} ");
                }

                message.Remove(message.Length - 1, 1);
                Console.WriteLine(message);
            }
        }

        private static void Task2()
        {
            var sr = File.ReadAllLines("..\\..\\messages.txt");
            for (var i = 0; i < sr.Length; i++)
            {
                sr[i] = Regex.Replace(sr[i], @"\s+", " ").Trim();
            }

            Console.WriteLine("Сообщения без слов, оканчивающихся на символ a");
            Message.ExeptChEnding('a', sr);
            Console.WriteLine("Все сообщения с количеством букв менее 6");
            Message.LessThenNWords(6, sr);
            Console.WriteLine("Сообщения из самых длинных слов в сообщениях");
            Message.MessageOfLongestWord(sr);
        }

        #endregion

        #region Task3

        /*
         * 3. *Для двух строк написать метод, определяющий,
         * является ли одна строка перестановкой другой.
         * Регистр можно не учитывать:
         * а) с использованием методов C#;
         * б) *разработав собственный алгоритм.
         * Например:
         * badc являются перестановкой abcd.
         * Павлов Алексей
         */


        private static bool IsSameWords(string s1, string s2)
        {
            var a = Regex.Replace(s1, @"\s+", "").ToCharArray();
            var b = Regex.Replace(s2, @"\s+", "").ToCharArray();
            Array.Sort(a);
            Array.Sort(b);
            return new string(a).Equals(new string(b));
        }

        private static bool IsSameWordsWithoutDefaultMethods(string s1, string s2)
        {
            if (s1.Length != s2.Length)
                return false;
            var letters = new Dictionary<char, int>();
            foreach (var ch in s2)
            {
                if (letters.ContainsKey(ch))
                {
                    letters[ch]++;
                }
                else
                {
                    letters.Add(ch, 0);
                }
            }

            foreach (var key in letters)
            {
                var lettCount = 0;
                foreach (var ch in s1)
                {
                    if (ch == key.Value)
                    {
                        lettCount++;
                    }
                }

                if (lettCount != key.Value)
                    return false;
            }

            return true;
        }

        private static void Task3()
        {
            Console.WriteLine("Введите 2 строки для сравнения");
            var s1 = Console.ReadLine();
            var s2 = Console.ReadLine();
            Console.WriteLine(IsSameWords(s1, s2));
            Console.WriteLine(IsSameWordsWithoutDefaultMethods(s1, s2));
        }

        #endregion

        #region Task4

        /*
        4. Задача ЕГЭ.
        *На вход программе подаются сведения о сдаче экзаменов учениками 9-х классов
        некоторой средней школы.
        В первой строке сообщается количество учеников N, которое не меньше 10, но не
        превосходит 100, каждая из следующих N строк имеет следующий формат:
        <Фамилия> <Имя> <оценки>,
        где <Фамилия> — строка, состоящая не более чем из 20 символов, <Имя> — строка, состоящая не
        более чем из 15 символов, <оценки> — через пробел три целых числа, соответствующие оценкам по
        пятибалльной системе. <Фамилия> и <Имя>, а также <Имя> и <оценки> разделены одним пробелом.
        Пример входной строки:
        Иванов Петр 4 5 3
        Требуется написать как можно более эффективную программу, которая будет выводить на экран
        фамилии и имена трёх худших по среднему баллу учеников. Если среди остальных есть ученики,
        набравшие тот же средний балл, что и один из трёх худших, 
        следует вывести и их фамилии и имена.
        Достаточно решить 2 задачи. Старайтесь разбивать программы на подпрограммы. Переписывайте в
        начало программы условие и свою фамилию. Все программы сделать в одном решении. Для решения
        задач используйте неизменяемые строки (string)
        Павлов Алексей
        */

        private struct Student : IComparable
        {
            private int[] _grades;
            private string _name;
            private double _avarage;

            public Student(int[] grades, string name, double avarage)
            {
                _grades = grades;
                _name = name;
                _avarage = avarage;
            }

            public int CompareTo(object obj)
            {
                if (obj is Student arr)
                {
                    return this._avarage.CompareTo(arr._avarage);
                }

                throw new ArgumentException("object is not Student");
            }

            public override string ToString()
            {
                var sGrades = string.Empty;
                foreach (var grade in _grades)
                {
                    sGrades += $"{grade.ToString()} ";
                }

                sGrades = sGrades.Remove(sGrades.Length - 1);
                return $"{_name} оценки: {sGrades}, средняя: {_avarage}";
            }

            public static bool operator ==(Student s1, Student s2)
            {
                return (Math.Abs(s1._avarage - s2._avarage) < 0.000001);
            }

            public static bool operator !=(Student s1, Student s2)
            {
                return !(s1 == s2);
            }
        }

        private static Student[] GetGrades(string filename)
        {
            var sr = File.ReadAllLines(filename);
            var student = new Student[sr.Length - 1];
            var j = 0;
            for (var i = 1; i < sr.Length; i++)
            {
                var words = sr[i].Split(' ');
                var grades = new[]
                {
                    int.Parse(words[2]),
                    int.Parse(words[3]),
                    int.Parse(words[4])
                };
                student[j++] = new Student(grades,
                    $"{words[0]} {words[1]}",
                    GetAvarage(grades));
            }

            return student;
        }

        private static double GetAvarage(int[] grades)
        {
            return (double)grades.Sum()/(double)grades.Length;
        }

        private static void Task4()
        {
            var student = GetGrades("..\\..\\ClassGrades.txt");
            Array.Sort(student);
            var i = 0;
            
            while (((i <= 3) || (student[i] == student[i + 1])) && (i != student.Length))
            {
                Console.WriteLine(student[i++]);
            }
        }

        #endregion

        #region Task5

        /*
        5. **Написать игру «Верю. Не верю». В файле хранятся вопрос и ответ, правда это или нет.
        Например: «Шариковую ручку изобрели в древнем Египте», «Да». 
        Компьютер загружает эти данные, случайным образом выбирает 5 вопросов и задаёт их игроку. 
        Игрок отвечает Да или Нет на каждый вопрос и набирает баллы за каждый правильный ответ.
        Список вопросов ищите во вложении или воспользуйтесь интернетом.
        Павлов Алексей
        */

        private readonly struct Question
        {
            private readonly string _question;
            private readonly bool _answer;

            public Question(string question, bool answer)
            {
                this._question = question;
                this._answer = answer;
            }

            public bool ChekAnswer(string answer)
            {
                var a = Regex.Replace(answer, @"\s+", "").ToLower();
                return _answer ? a.Equals("да") : a.Equals("нет");
            }
            
            public override string ToString()
            {
                return $"{_question}";
            }
        }

        private static Question[] GetQuestionsFromFile(string filename)
        {
            var sr = File.ReadAllLines(filename);
            var questions = new Question[sr.Length];
            var i = 0;
            foreach (var s in sr)
            {
                var words = s.Split('?');
                words[1] = words[1].Remove(words[1].Length - 1, 1)
                    .Remove(0, 2);
                
                questions[i++] = new Question(words[0],
                    words[1].Equals("Да", StringComparison.OrdinalIgnoreCase));
            }

            return questions;
        }

        private static void Task5()
        {
            var questions = GetQuestionsFromFile("..\\..\\TrueOrFaulse.txt").ToList();
            var i = 0;
            var j = 0;
            var score = 0;
            var rnd = new Random();
            while (i <= 5)
            {
                j = rnd.Next(0, questions.Count);
                Console.WriteLine(questions[j]);
                var answer = Console.ReadLine();
                if (questions[j].ChekAnswer(answer))
                {
                   Console.WriteLine("Верно");
                   score++;
                }
                else
                {
                    Console.WriteLine("Неверно");
                }
                questions.RemoveAt(j);
                i++;
            }
            Console.WriteLine($"Ваш счет: {score}");
        }
        
        #endregion

        public static void Main(string[] args)
        {
            Task1();
            Console.ReadKey();
            Console.Clear();
            Task2();
            Console.ReadKey();
            Console.Clear();
            Task3();
            Console.ReadKey();
            Console.Clear();
            Task3();
            Console.ReadKey();
            Console.Clear();
            Task4();
            Console.ReadKey();
            Console.Clear();
            Task5();
        }
    }
}