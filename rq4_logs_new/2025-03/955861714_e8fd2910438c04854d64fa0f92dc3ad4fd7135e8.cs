using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Passcracker
{
    class Program
    {
        static string PassAttempter(string chars, string Password, string pass)
        {
            Random res = new Random();
            string[] temp = { };

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            do
            {
                pass = "";
                for (int i = 0; i < Password.Length; ++i)
                {
                    int x = res.Next(chars.Length);
                    pass = pass + chars[x];
                }
                Console.WriteLine(pass);
                if (temp.Contains(pass))
                {
                    Console.WriteLine("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                    continue;
                }
                
                temp.Append(pass);

            } while (pass != Password);

            TimeSpan ts = stopWatch.Elapsed;

            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}",
                ts.Minutes, ts.Seconds,
                ts.Milliseconds);
            Console.WriteLine("RunTime " + elapsedTime);

            return pass;
        }


        static void Main(string[] args)
        {
            const string Password = "Riza123@!";
            string pass = "";
            string chars = "iazR123!@";
            string passcheck = PassAttempter(chars, Password,pass);
            if(passcheck == Password)
            {
                Console.WriteLine("Pass is cracked.");
                Console.WriteLine(pass);
            }
            else
            {
                Console.WriteLine("Pass is not cracked.");

            }

        }
    }
}