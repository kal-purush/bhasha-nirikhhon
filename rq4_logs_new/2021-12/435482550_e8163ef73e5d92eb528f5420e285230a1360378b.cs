using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DoWhile_While
{
    class Program
    {
        static void Main(string[] args)
        {
            int sayi = 0;
            //do
            //{
            //    sayi++;
            //    if (sayi == 7)
            //    {
            //        continue;
            //    }
            //    Console.WriteLine(sayi);
            //} while (sayi < 10);

            while (sayi < 10)
            {
                sayi++;
                if (sayi == 7)
                {
                    continue;
                }
                Console.WriteLine(sayi);
            }

            Console.ReadLine();
        }
    }
}