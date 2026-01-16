using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CodeBlocks.Topla;

namespace CodeBlocks
{
    class Program
    {
        static void Main(string[] args)
        {
            int sayi = 0;
            sayi = Toplama.Toplam(50);
            Console.WriteLine(sayi);
            Console.ReadLine();
        }

        //static int Toplam(int a)
        //{
        //    int b = 10;
        //    return a + b;
        //}

    }
}

namespace CodeBlocks.Topla
{
    class Toplama
    {
        public static int Toplam(int a)
        {
            int b = 10;
            return a + b;
        }
    }
}