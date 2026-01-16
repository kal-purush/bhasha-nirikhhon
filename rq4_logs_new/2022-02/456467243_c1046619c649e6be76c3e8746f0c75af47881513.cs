using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RekurzivHatvany {
    class Program {
        public static double Hatvany(double x, double y) {
            if (y == 0) return 1.0;
            else return x * Hatvany(x, y - 1);
        }

        static void Main(string[] args) {
            Console.WriteLine(Hatvany(2, 10));
            Console.ReadKey();
        }
    }
}