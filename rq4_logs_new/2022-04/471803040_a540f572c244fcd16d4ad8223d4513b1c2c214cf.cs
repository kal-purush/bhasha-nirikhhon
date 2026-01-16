using System;

namespace BolmeIsaretsiz
{
    internal class Program
    {
        static void Main(string[] args)
        {

            bolum(12, 2);
            carpma(5, 4);


        }
        static void bolum(int sayi, int bolum)
        {

            int sayac = 0;
            int sonuc = 0;

            for (int i = sayi; i > 0; i--)
            {
                sayac++;
                if (sayac == bolum)
                {
                    sonuc++;
                    sayac = 0;
                }
            }
            Console.WriteLine(sonuc);
        }

        static void carpma(int sayi, int carpim)
        {
            int sayac = 0;
            int sonuc = 0;

            if (sayi < 0)
            {
                sayi = sayi - (sayi + sayi);
                while (true)
                {
                    sayac++;
                    if (sayac == sayi)
                    {
                        carpim--;
                        sonuc += sayac;
                        sayac = 0;
                    }
                    if (carpim == 0)
                    {
                        break;
                    }
                }
                sonuc = sonuc - (sonuc + sonuc);
            }
            else if (sayi > 0)
            {
                while (true)
                {
                    sayac++;
                    if (sayac == sayi)
                    {
                        carpim--;
                        sonuc += sayac;
                        sayac = 0;
                    }
                    if (carpim == 0)
                    {
                        break;
                    }
                }
            }
            else
            {
                sonuc = 0;
            }
            
            Console.WriteLine(sonuc);
        }
    }
}