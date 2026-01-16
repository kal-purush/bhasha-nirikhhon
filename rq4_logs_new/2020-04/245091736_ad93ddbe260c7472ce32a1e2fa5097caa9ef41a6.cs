using System;
using System.Collections.Generic;
using System.Linq;

namespace RailFenceCipher
{
    public class RailFenceCipher
    {
        public static string Encode(string inputText, int railsCount)
        {
            List<List<char>> rails = new List<List<char>>();

            for (int i = 0; i < railsCount; i++)
            {
                rails.Add(new List<char>());
            }

            while (inputText.Length != 0)
            {
                for (int i = 0; i < railsCount; i++)
                {
                    rails[i].Add(inputText[0]);
                    inputText = inputText.Remove(0, 1);
                    if (inputText.Length == 0)
                        return rails.Select(item => new string(item.ToArray())).Aggregate((outText, textToAdd) => outText + textToAdd);
                }

                for (int i = railsCount - 1; i > 1; i--)
                {
                    rails[i - 1].Add(inputText[0]);
                    inputText = inputText.Remove(0, 1);
                    if (inputText.Length == 0)
                        return rails.Select(item => new string(item.ToArray())).Aggregate((outText, textToAdd) => outText + textToAdd);
                }
            }

            return rails.Select(item => new string(item.ToArray())).Aggregate((outText, textToAdd) => outText + textToAdd);
        }

        public static string Decode(string inputText, int railsCount)
        {
            List<List<char>> rails = new List<List<char>>();
            string tempText = inputText;
            for (int i = 0; i < railsCount; i++)
            {
                rails.Add(new List<char>());
            }

            while (tempText.Length != 0)
            {
                for (int i = 0; i < railsCount; i++)
                {
                    rails[i].Add(tempText[0]);
                    tempText = tempText.Remove(0, 1);
                    if (tempText.Length == 0)
                        break;
                }

                if (tempText.Length == 0)
                    break;

                for (int i = railsCount - 1; i > 1; i--)
                {
                    rails[i - 1].Add(tempText[0]);
                    tempText = tempText.Remove(0, 1);
                    if (tempText.Length == 0)
                        break;
                }
            }

            tempText = inputText;

            string result = "";

            for (int i = 0; i < rails.Count; i++)
            {
                for (int j = 0; j < rails[i].Count; j++)
                {
                    if (tempText.Length != 0)
                    {
                        rails[i][j] = tempText[0];
                        tempText = tempText.Remove(0, 1);
                    }
                    else
                    {
                        break;
                    }
                }
            }

            while (rails.Count != 0)
            {
                for (int i = 0; i < rails.Count; i++)
                {
                    if (rails[i].Count == 0)
                    {
                        rails.RemoveAt(i);
                        i++;
                        break;
                    }
                    result += rails[i][0];
                    rails[i].RemoveAt(0);
                }


                for (int i = rails.Count - 2; i >= 1; i--)
                {
                    if (rails[i].Count == 0)
                    {
                        rails.RemoveAt(i);
                        i--;
                        break;
                    }

                    result += rails[i][0];
                    rails[i].RemoveAt(0);
                }
            }

            return result;
        }
    }
}