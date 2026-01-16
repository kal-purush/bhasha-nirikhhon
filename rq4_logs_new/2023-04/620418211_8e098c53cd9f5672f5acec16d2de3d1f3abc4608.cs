using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Classwork_3
{
    internal class Program
    {
        static void Main(string[] args)
        {
            task1();
            string name = "";
            string initial = "HEllo";
            Console.WriteLine(initial[0]);
            Console.WriteLine(initial.Length);
            string name2 = string.Empty;
            string name3 = null;
            string name4 = name3;
            name3 = "rwererw";

            Console.WriteLine(name4+name3+initial.Length);
            int? ad = null;

             string newst=string.Concat("hello", "my");
             string[] arrayofstring = { "wer", "rew", "fdf" };
             string result = string.Join(" 11 ", arrayofstring);
            Console.WriteLine(result);

            string bankcard = " #   123 45 667   *  ";
            string[] arrayofblocks= bankcard.Split(' ');
            string trimbank = bankcard.Trim();
            trimbank = bankcard.Trim('*','#');
            Console.WriteLine(trimbank);

            int a = 1;
            int b = 2;
            Console.WriteLine("fdffdf {0}", a);
            string formated = string.Format("fdffdf! 12222222 {0}", a);

            Console.WriteLine(formated);

            int dollars = 100;
            string frmdollars = string.Format("my variable {0:C0}", dollars);
             frmdollars = string.Format("my variable {0:f1}", dollars);
             frmdollars = string.Format("my variable {0:f2}", dollars);


            double dollars2 = 23.5;
            string formatedstr1 = $"{a} + {b} = {a + b}";
            string newstr= formated.Insert(0, name);
            newstr = formated.Insert(formated.Length, name);
            int index =formated.IndexOf("!");
            string removestr = formated.Remove(index, 2);
            string removestr2 = formated.Replace("!", name);
            formated= formated.ToLower();
            formated = formated.ToUpper();
            Console.WriteLine(removestr);

            string[] files = new string[]
            {
                "1.exe",
                "2.exe",
                 "3.txtr"
            };
            for (int i = 0; i < files.Length; i++)
            {
                if (files[i].EndsWith("exe"))
                {
                    Console.WriteLine(files[i]);
                }
               
            }
            bool bb = formated.Contains("!");
            formated = formated.Substring(5); //верезать и вернуть


            int result2 = string.Compare(formated, formatedstr1);
            /// >0 , <0 ,= 0
            
             bool br = string.Equals(formated, formatedstr1);

             br = formated == formatedstr1;
            Console.ReadLine();
            ///stringbilder
            ///
            string.IsNullOrEmpty(formatedstr1);
            string.IsNullOrWhiteSpace(formatedstr1); //"" - null here for that
        }
      
        /// <summary>
        /// sadsadasdsadsad
        /// </summary>
        /// <exception></exception>

        private static void task1()
        {
            int[] arr = { 1, 2, 3, 4, 5 };
            int[] arr2 = new int[6];
            int i = 0;
            arr2.Append(i);

            string randomStr = " sdfdfds fsed sfd sdf";
            string randomStr2 = randomStr.Replace("abc", "*");
            string phrase = " my secrret is *SuperSecret*";
            int index1 = phrase.IndexOf('*');
            int index2 = phrase.IndexOf('*');
            string secret = phrase.Substring(index1+1, index2-index1-1);

            string newstr3 = phrase.Replace("S", "!", IgnoreCase: true, Cultureinfo.InvarianCulture);
            string[] words = phrase.Split(' ');

            for(int i=0; i<words.Length; i++)
            {
                char letter = words[i][0];
                string newstr4 = words[i].Replace(letter, char.ToUpper(letter));
                words[i] = newstr4;
            }
            string result = string.Join(" ", words);
            Regex regex = new Regex("[0-9]{4}-[0-9]{4}");
            Match match = regex.Match(result);
        }   
    }
}