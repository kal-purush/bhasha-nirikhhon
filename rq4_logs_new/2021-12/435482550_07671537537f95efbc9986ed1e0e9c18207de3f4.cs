using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BoolOrnek
{
    class Program
    {
        static void Main(string[] args)
        {
            int sayi = 5;
            string isim = "Nuh Aktürk";
            Console.WriteLine("a" == "a"); //2 tane string karşılaştırma | Çıktısı= True
            Console.WriteLine("b" == "B"); // False
            Console.WriteLine(1.2f == 1.3f); // False
            Console.WriteLine(1 == sayi); // False
            Console.WriteLine(1 != sayi); // True
            Console.WriteLine(isim.Contains("Nu")); // True
            Console.ReadLine();
        }
    }
}