using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lab1
{
    class ConsoleMenu
    {
        private List<string> optionsList;
        private int longestOptionLength;
        private int chosenLine = 0;


        public ConsoleMenu()
        {
            optionsList = new List<string>();
        }

        public void AddOption(string option)
        {
            optionsList.Add(option);
            longestOptionLength = GetLongestOptionLength();
        }

        public string RenderAll(bool border = true)
        {
            StringBuilder stringBuilder = new StringBuilder();

            if (border)
            {
                stringBuilder.Append('=', longestOptionLength/2 + 1);
                stringBuilder.Append("Menu");
                stringBuilder.Append('=', longestOptionLength/2 + 1);
                stringBuilder.Append('\n', 2);
            }

            for (int i = 0; i < optionsList.Count; i++)
            {
                stringBuilder.Append(RenderLine(i) + "\n");
            }

            if (border)
            {
                stringBuilder.Append('\n', 2);
                stringBuilder.Append('=', longestOptionLength + 6);
            }


            return stringBuilder.ToString();
        }

        public string RenderLine(int index, bool checkbox = true)
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

        public static T Clamp<T>(T value, T min, T max) where T : IComparable<T>
        {
            if (value.CompareTo(min) < 0)
                return min;
            if (value.CompareTo(max) > 0)
                return max;
            return value;
        }
    }
}