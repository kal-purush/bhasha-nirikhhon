using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Arrays
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
            string[] siparisNo = new string[3]; //memory de array boyutu kadar yer açılır.

            siparisNo[0] = "INZ123";
            siparisNo[1] = "GOOGLE2";
            siparisNo[2] = "GOOGLE3";

            siparisNo[0] = "INZ345"; //değişiklik
            */

            string[] siparisNo = {"birinci siparis", "ikinci siparis", "ucuncu siparis"};

            Console.WriteLine(siparisNo[2]);
            Console.WriteLine("Dizi Uzunluğu: " + siparisNo.Length);
            Console.ReadLine();
        }
    }
}