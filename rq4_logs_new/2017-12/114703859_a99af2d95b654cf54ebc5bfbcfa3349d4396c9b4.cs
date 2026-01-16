using System;

namespace GenerateAcronym
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            Console.WriteLine(GenerateAcronym("The Federal Bureau of Investigation"));
            Console.ReadLine();
        }

        public static string GenerateAcronym(string title)
        {
            string acronym = "";
            bool temp = false;
            if (title == "") return null;
            string[] doNotUse = { "a", "for", "an", "and", "of", "or", "the", "to", "with" };
            string[] titleArray = title.ToLower().Split(' ');
            foreach (string word in titleArray)
            {
                temp = false;
                for (int i = 0; i < doNotUse.Length; i++)
                {
                    if (word == doNotUse[i])
                    {
                        temp = true;
                        break;
                    }
                }
                if (temp) continue;
                acronym += char.ToUpper(word[0]);
            }
            return acronym;
        }
    }
}