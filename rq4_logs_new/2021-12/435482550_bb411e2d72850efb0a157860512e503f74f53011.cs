using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OperandlarlaIslemler
{
    class Program
    {
        static void Main(string[] args)
        {
            int ilkSayi = 35;
            int ikinciSayi = 34;
            int toplama = 35 + 34;
            int cikarma = 35 - 34;
            int carpim = 35 * 34;
            //int bolme = 35 / 34;
            float bolme = 35f / 34f;
            int mod = ilkSayi % ikinciSayi;
            float bolmeIkinci = (float)ilkSayi / ikinciSayi;
            Console.WriteLine("Toplam: " + toplama + "\nÇıkarma: " + cikarma + "\nÇarpım: " + carpim + "\nBolme: " + bolmeIkinci + "\nMod: " + mod);
            Console.WriteLine("******************************\n");

            int ornek1 = 5 + 6 * 7;
            int ornek2 = (5 + 6) * 5;
            Console.WriteLine(ornek1); //47
            Console.WriteLine(ornek2); //55
            Console.WriteLine("******************************\n");

            int ucuncuSayi = ilkSayi + 5;
            ucuncuSayi += 10;      //ucuncuSayi = ucuncuSayi + 10;

            ucuncuSayi -= 10;   // 40
            ucuncuSayi *= 2;   //80
            ucuncuSayi /= 5;   //16
            Console.WriteLine(ucuncuSayi);
            Console.WriteLine("******************************\n");

            int dorduncuSayi = 5;

            dorduncuSayi++;   //5+1
            dorduncuSayi--;   //6-1
            ++dorduncuSayi;   //5+1
            Console.WriteLine(dorduncuSayi);

            Console.ReadLine();
        }
    }
}