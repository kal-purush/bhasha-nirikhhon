using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SwitchCase
{
    class Program
    {
        static void Main(string[] args)
        {
            int golSayisi = 0;
            string takimSeviyesi = "";
            Random sayi = new Random();
            golSayisi = sayi.Next(0, 6); //0 ve 5 sayıları gelebilir.

            switch (golSayisi)
            {
                case 1:
                    takimSeviyesi = "OK";
                    break;
                case 2:
                    takimSeviyesi = "Güzel";
                    break;
                case 3:
                    takimSeviyesi = "Yüksek";
                    break;
                case 4:
                    takimSeviyesi = "Muhteşem";
                    break;
                case 5:
                    takimSeviyesi = "WOW";
                    break;
                default:
                    takimSeviyesi = "Kötü";
                    break;
            }

            Console.WriteLine(takimSeviyesi);
            Console.ReadLine();
        }
    }
}