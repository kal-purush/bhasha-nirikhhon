using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsharpJikken
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("hello " + DateTime.Now);
            File.WriteAllText("hoge.txt", "world + " + DateTime.Now + "\n");
        }
    }
}