using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StringInterpolation
{
    class Program
    {
        static void Main(string[] args)
        {
            string birinci = "Nuh"; 
            string ikinci = "Oyun ve Uygulama Akademisi katılımcısı " + birinci;
            string ucuncu = "Oyun ve Uygulama Akademisi katılımcısı " + birinci + " " + "Aktürk";
            string dorduncu = $"{birinci} {ikinci} elle yazi yaziyorum";
            string besinci = $@"c:\users\nuh\{ikinci}\veri";
            Console.WriteLine(besinci);
            Console.ReadLine();
        }
    }
}