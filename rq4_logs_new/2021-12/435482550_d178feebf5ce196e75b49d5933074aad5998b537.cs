using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StringConcatanate
{
    class Program
    {
        static void Main(string[] args)
        {
            string birinci = "Nuh";
            string ikinci = "Oyun ve Uygulama Akademisi katılımcısı " + birinci;
            string ucuncu = "Oyun ve Uygulama Akademisi katılımcısı " + birinci + " " + "Aktürk";
            Console.WriteLine(ucuncu);
            Console.ReadLine();
        }
    }
}