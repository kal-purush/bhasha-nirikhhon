using System;

namespace MinimumEditDistance
{
    public class program
    {
        public const int Errnum = -1;
        public static void Main()
        {
            Console.WriteLine("debugging");
        }
        public static int MED(string str1, string str2)
        {
            if (str1 == null && str2 == null)
            {
                Console.WriteLine("The input string is empty");
                return Errnum;
            }
            else
            {
                int[,] table = new int[str1.Length + 1, str2.Length + 1];
                for (int i = 0; i < str1.Length + 1; i++)
                {
                    for (int j = 0; j < str2.Length + 1; j++)
                    {
                        //the length should be string length + 1!
                        if (i == 0)
                        {
                            table[i, j] = j;
                        }
                        else if (j == 0)
                        {
                            table[i, j] = i;
                        }
                        //set the frist row and column
                        else if (str1[i - 1] == str2[j - 1])
                        {
                            table[i, j] = table[i - 1, j - 1];
                        }
                        //if the char in i-1 is the same as char in j-1, there is no operation needed
                        else
                        {
                            table[i, j] = Math.Min(
                                Math.Min(
                                    table[i - 1, j] + 1,
                                    //delete str1[i]
                                    table[i, j - 1] + 1
                                //insert str2[j] to str1
                                ), table[i - 1, j - 1] + 1);
                            //substitute str1[i] by str2[j]
                        }
                    }
                }
                //Console.WriteLine("The distance between {0} and {1} is {2}", str1, str2, table[str1.Length - 1, str2.Length - 1]);
                //debug
                return table[str1.Length, str2.Length];
            }
        }
    }
}