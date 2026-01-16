using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace _8_1
{
    class Program
    {
        static void Main(string[] args)
        {
            Tester t = new Tester();
            t.run();
        }
    }
    class Tester
    {
        public void run()
        {
            int intVer = tripler(3);
            float floatVer = tripler(3.1F);
            Console.WriteLine("{0} {1}", intVer, floatVer);
        }
        private static int tripler(int num)
        {
            return num *= 3;
        }
        private static float tripler(float num)
        {
            return num *= 3;
        }
    }
}