using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
   
    class Program
    {
        static string passwordBreaker(string passwd)
        {
            int[] passwdBreaker = new int[passwd.Length];
            char[] passwdBreakerChar = new char[passwd.Length];
            for (int i = 0; i <= passwd.Length - 1; i++)
            {
                for (int j = 1; j <= 128; j++)
                {
                    if (j == passwd[i])
                    {
                        passwdBreaker[i] = j;
                        passwdBreakerChar[i] = Convert.ToChar(passwdBreaker[i]);
                    }
                }
            }
            string brokenPasswd = new string(passwdBreakerChar);
            return brokenPasswd;
        }


        static int[] generujKule(int iloscKul)
        {
            int[] kule = new int[iloscKul+1];

            for (int i = 1; i < iloscKul+1; i++)
            {
                kule[i] = i;
            }

            return kule;
        }
        static void losowanie (int liczbaKul)
        {
            int[] kule = generujKule(liczbaKul);
            var random = new Random();
            int losowa = 0;
            int[] wylosowaneLiczby = new int[7];

            for (int i = 0; i <= 6; i++)
            {
                losowa = random.Next(1, liczbaKul);
                wylosowaneLiczby[i] = kule[losowa];
                for (int j = losowa; j < liczbaKul; j++)
                {
                    kule[j] = kule[j + 1];
                }
            }

            Console.WriteLine("Wylosowane liczby to:");
            for (int i = 0; i <= 6; i++)
            {
                Console.WriteLine(wylosowaneLiczby[i]);
            }
        }






  
        static void Main(string[] args)
        {

            string passwd = "adsfs7*((89dsfsfd<sdf";
            Console.Write(passwd);
            Console.WriteLine("Złamane hasło to: "+passwordBreaker(passwd));
            losowanie(49);
            Console.ReadKey();
        }
    }

}