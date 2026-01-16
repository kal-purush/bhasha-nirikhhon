using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Toplama
{
    class Program
    {
        static void Main(string[] args)
        {
            int birinci = 35;
            int ikinci = 34;
            Console.WriteLine(birinci + ikinci); // 69
            Console.WriteLine(birinci + ikinci + "bir string"); // 69bir string
            Console.WriteLine(birinci + "bir string" + ikinci); // 35bir string34
            Console.WriteLine(birinci + "bir string" + ikinci + 10); // 35bir string3410
            Console.WriteLine((birinci + 10) + "bir string" + ikinci + 10); // 45bir string3410
            Console.ReadLine();
        }
    }
}