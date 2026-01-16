using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechWeekChallenge2
    {
    public class TechWeekChallenge3
        {
        public static void Main (string[] args)
            {
            string ip1 = Console.ReadLine();
            string output = ReverseSignOfWords(ip1);
            Console.WriteLine(output);
            Console.ReadKey();
            }

        private static string ReverseSignOfWords (string ip1)
            {
            string sb = string.Empty;
            string[] ipArr = ip1.Split(' ');
            for ( int i = ipArr.Length - 1; i >= 0; i-- )
                {
                if(i == 0)
                    sb += (ipArr[i]);
                else
                    sb += (ipArr[i] + " ");
                }
            return sb;
            }
        }
    }