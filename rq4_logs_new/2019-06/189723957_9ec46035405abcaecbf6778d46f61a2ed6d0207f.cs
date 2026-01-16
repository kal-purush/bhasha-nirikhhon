using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BMO.GameDevUnity.CSharp1.Pract5
{
    public static class Message
    {
        private static void Print(string message)
        {
            Console.WriteLine($"Сообщение: {message}");
        }

        private static void Print(string[] messageArray)
        {
            Console.Write("Сообщение: ");
            for (int i = 0; i < messageArray.Length; i++)
            {
                Console.Write($"{messageArray[i]} ");
            }
            Console.WriteLine();
        }

        public static void NoMore(string message, int length)
        {
            string[] arrayMessage = message.Split(' ', '\n', '\r');
            string[] arrayNoMore = new string[0];
            int j = 0;
            for (int i = 0; i < arrayMessage.Length; i++)
            {
                if (arrayMessage[i].Length <= length)
                {
                    Array.Resize(ref arrayNoMore, arrayNoMore.Length + 1);
                    arrayNoMore[j] = arrayMessage[i];
                    j++;
                }
            }
            Print(arrayNoMore);
        }

        public static void Delete(ref string message, char symbol)
        {
            string[] arrayMessage = message.Split(' ', '\n', '\r');
            string[] arrayDelete = new string[0];
            message = "";
            for (int i = 0; i < arrayMessage.Length; i++)
            {
                if (arrayMessage[i][arrayMessage[i].Length-1] != symbol)
                {
                    message += arrayMessage[i] + " ";
                }
            }
            Print(message);
        }

        public static void MoreLength(string message)
        {
            string[] arrayMessage = message.Split(' ', '\n', '\r');
            string max = arrayMessage[0];
            for (int i = 0; i < arrayMessage.Length-1; i++)
            {
                if (arrayMessage[i+1].Length > max.Length)
                {
                    max = arrayMessage[i + 1];
                }
            }
            Console.WriteLine($"Самое длинное слово сообщения: {max}");
        }

        public static void FrequencyAnalysis(string[] arrayWords, string message)
        {
            Dictionary<string, int> frequencyWords = new Dictionary<string, int>();
            string[] arrayMessage = message.Split(' ', '\n', '\r');
            Console.WriteLine("Результаты частотного анализа: ");
            for (int i = 0; i < arrayWords.Length; i++)
            {
                frequencyWords[arrayWords[i]] = 0;
                for (int j = 0; j < arrayMessage.Length; j++)
                {
                    if (arrayWords[i] == arrayMessage[j])
                    {
                        frequencyWords[arrayWords[i]]++;
                    }
                }
                Console.WriteLine($"Слово \"{arrayWords[i]}\" встречается в тексте {frequencyWords[arrayWords[i]]} раз(а)");
            }

        }

    }
}