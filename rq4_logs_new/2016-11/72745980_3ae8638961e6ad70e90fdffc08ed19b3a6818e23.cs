using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ConsoleHelper
{
    class ConsoleMenu
    {
        private List<string> optionsList;
        private bool border;
        private string title;
        private bool checkbox;
        private int longestOptionLength = 0;
        private int chosenLine = -1;


        public ConsoleMenu(bool checkbox = true, bool border = false, string title = "")
        {
            optionsList = new List<string>();
            this.checkbox = checkbox;
            this.border = border;
            this.title = title;
        }

        public void AddOption(string option)
        {
            optionsList.Add(option);
            chosenLine = -0;
            longestOptionLength = GetLongestOptionLength();
        }

        public void SetOptions(IEnumerable<string> enumerable)
        {
            foreach (var el in enumerable)
            {
                optionsList.Add(el);
            }
            chosenLine = -0;
            longestOptionLength = GetLongestOptionLength();
        }

        public bool Border
        {
            get { return border; }
            set { border = value; }
        }

        public string Title
        {
            get { return title; }
            set { title = value; }
        }

        public bool Checkbox
        {
            get { return checkbox; }
            set { checkbox = value; }
        }

        public string RenderAll()
        {
            StringBuilder stringBuilder = new StringBuilder();

            if (border)
            {
                stringBuilder.Append('=', longestOptionLength/2 + 1);
                stringBuilder.Append(title);
                stringBuilder.Append('=', longestOptionLength/2 + 1);
                stringBuilder.Append('\n', 2);
            }

            if (optionsList.Count > 0)
            {
                for (int i = 0; i < optionsList.Count; i++)
                {
                    stringBuilder.AppendLine(RenderLine(i));
                }
            }
            else
            {
                stringBuilder.AppendLine("No options available");
            }

            if (border)
            {
                stringBuilder.Append('\n', 2);
                stringBuilder.Append('=', longestOptionLength + title.Length + 2);
            }


            return stringBuilder.ToString();
        }

        public string RenderLine(int index)
        {
            if (index != Clamp(index, 0, optionsList.Count))
                return null;

            StringBuilder stringBuilder = new StringBuilder();
            string s = optionsList[index];
            stringBuilder.Append(s);

            if (checkbox)
            {
                int spaceLeft = longestOptionLength - s.Length + 2;
                stringBuilder.Append(' ', spaceLeft);
                stringBuilder.Append("[");
                if (index == chosenLine)
                    stringBuilder.Append("X");
                else
                    stringBuilder.Append(" ");
                stringBuilder.Append("]");
            }

            return stringBuilder.ToString();
        }

        public void MoveUp()
        {
            chosenLine--;
            chosenLine = Clamp(chosenLine, 0, optionsList.Count - 1);
        }

        public void MoveDown()
        {
            chosenLine++;
            chosenLine = Clamp(chosenLine, 0, optionsList.Count - 1);
        }

        public int ChosenLine
        {
            get { return chosenLine; }
        }

        public int GetLongestOptionLength()
        {
            string longest = optionsList.OrderByDescending(s => s.Length).First();
            return longest.Length;
        }

        public T Clamp<T>(T value, T min, T max) where T : IComparable<T>
        {
            if (value.CompareTo(min) < 0)
                return min;
            if (value.CompareTo(max) > 0)
                return max;
            return value;
        }
    }

    static class ConsoleInput
    {
        public static int GetMenuInput(ConsoleMenu menu)
        {
            Console.CursorVisible = false;
            int originLeft = Console.CursorLeft;
            int originTop = Console.CursorTop;
            while (true)
            {
                Console.SetCursorPosition(originLeft, originTop);
                Console.Write(menu.RenderAll());
                ConsoleKey key = Console.ReadKey().Key;
                switch (key)
                {
                    case ConsoleKey.UpArrow:
                        menu.MoveUp();
                        break;
                    case ConsoleKey.DownArrow:
                        menu.MoveDown();
                        break;
                    case ConsoleKey.Enter:
                        Console.CursorVisible = true;
                        return menu.ChosenLine;
                }
            }
        }

        public static string GetString(String message, Regex regex = null, ConsoleKey exitKey = ConsoleKey.Escape)
        {
            if (regex == null)
            {
                regex = new Regex(".+");
            }
            while (true)
            {
                Console.Write(message);
                string s = Console.ReadLine();
                if (regex.IsMatch(s))
                {
                    return s;
                }
                else
                {
                    ColoredWriteline("Invalid Input!", foreColor: ConsoleColor.Red);
                }
            }
        }

        public static int GetInt32(string message)
        {
            while (true)
            {
                try
                {
                    Console.Write(message);
                    string str = Console.ReadLine();
                    int i = Int32.Parse(str);
                    return i;
                }
                catch (Exception ex)
                {
                    if (
                        ex is ArgumentNullException
                        || ex is FormatException
                        || ex is OverflowException
                    )
                    {
                        ColoredWriteline("Input must be interger!", foreColor: ConsoleColor.Red);
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }
        }

        public static T GetEnum<T>() where T : struct, IConvertible
        {
            if (typeof(T).IsEnum)
            {
                ConsoleMenu enumMenu = new ConsoleMenu();
                string[] names = Enum.GetNames(typeof(T));
                enumMenu.SetOptions(names);
                int choice = GetMenuInput(enumMenu);
                return (T) (object) choice;
            }
            else
            {
                throw new ArgumentException("Type must be Enum");
            }
        }

        public static void WaitForKey()
        {
            Console.CursorVisible = false;
            ColoredWriteline("Press any key to continue", foreColor: ConsoleColor.Green);
            Console.ReadKey();
            Console.CursorVisible = true;
        }


        public static void ColoredWrite(
            string message,
            ConsoleColor foreColor = ConsoleColor.White,
            ConsoleColor backColor = ConsoleColor.Black)
        {
            Console.ForegroundColor = foreColor;
            Console.BackgroundColor = backColor;
            Console.Write(message);
            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.Black;
        }

        public static void ColoredWriteline(
            string message,
            ConsoleColor foreColor = ConsoleColor.White,
            ConsoleColor backColor = ConsoleColor.Black)
        {
            ColoredWrite(message + "\n", foreColor, backColor);
        }
    }
}