using System;
using System.IO;
namespace Totaler
{
    class Program
    {
        private static string path = "B:\\CodeStuff\\C#\\Advent\\1\\input.txt";
        static void Main(string[] args)
        {
            StreamReader sr = new StreamReader(path);

            int ans = 0;
            while (!sr.EndOfStream)
            {
                string s = sr.ReadLine();
                int num = Convert.ToInt32(s.Substring(1));
                if (s[0] == '+')
                {
                    ans += num;
                }
                else
                {
                    ans -= num;
                }
            }
            Console.WriteLine(ans);
        }
    }
}