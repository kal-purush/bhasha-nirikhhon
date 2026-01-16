using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ForLoop
{
    class Program
    {
        static void Main(string[] args)
        {
            //for (int i = 10; i > 0; i++)
            //{
            //    Console.WriteLine(i);
            //}
            string[] siparisNo =
            {
                "INZ001",
                "GGLE01",
                "APP01"
            };

            for(int i = siparisNo.Length; i > 0; i--)
            {
                Console.WriteLine(siparisNo[i - 1]);
            }
            Console.WriteLine();
            Console.ReadLine();
        }
    }
}