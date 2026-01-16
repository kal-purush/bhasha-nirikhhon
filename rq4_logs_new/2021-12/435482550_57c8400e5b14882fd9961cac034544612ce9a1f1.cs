using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace YeniClassOlusturma
{
    class Program
    {
        static void Main(string[] args)
        {
            Yeni inst = new Yeni();

            inst.birinci = 10;
            inst.ikinci = 20;

            Console.WriteLine(inst.birinci + inst.ikinci);
            Console.ReadLine();
        }

        class Yeni
        {
            public int birinci;
            public int ikinci;
        }
    }
}