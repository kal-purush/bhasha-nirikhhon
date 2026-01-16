using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RekurzivFaktorialis {
    internal class Program {

        public static ulong Faktorialis(ulong x) {
            if (x == 0) return 1;
            else return x * Faktorialis(x - 1);

        }

        static void Main(string[] args) {

            while (true) {
                Console.WriteLine(Faktorialis(Convert.ToUInt64(Console.ReadLine())));
            }
            //Console.ReadKey();
        }
    }
}