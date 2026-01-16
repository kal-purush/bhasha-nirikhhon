using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BasitDortIslem
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
            Console.WriteLine("Toplam: " + toplama + "\nÇıkarma: "+ cikarma + "\nÇarpım: " + carpim + "\nBolme: " + bolmeIkinci + "\nMod: " + mod);

            int ornek1 = 5 + 6 * 7;
            int ornek2 = (5 + 6) * 5;
            Console.WriteLine(ornek1); //47
            Console.WriteLine(ornek2); //55

            Console.ReadLine();
        }
        
    }
    /* 
    * İşlem sırası
    1) Parantez ()
    2) Power(üstel sayılar)
    3) Çarpma ve bölme(soldan sağa)
    4) Toplama ve çıkarma(soldan sağa)
    */
}