using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HelloWorld
{
    class Program
    {
        static int a; //private - başka bir classtan erişilemez.
        static void Main(string[] args)
        {
            a = 1; //2 3 4 100
            Console.WriteLine(123);
            Console.WriteLine('B');
            //Console.WriteLine("Hello World!"); //Burası bir yorum, Bu fonksiyon console da Hello World Yazılmasını sağlar.
            //Console.WriteLine("Bu bir satırdır.");
            //Console.WriteLine("Bu da bir satırdır.");
            Console.ReadLine();
        }
    }
}