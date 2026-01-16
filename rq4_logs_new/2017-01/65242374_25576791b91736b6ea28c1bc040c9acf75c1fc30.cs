using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SoapTestII
{
    class Program
    {
        static void Main(string[] args)
        {
            WebRef.RestWebservice wr = new WebRef.RestWebservice();
            string returned = wr.HelloWorld();
            Console.WriteLine("Returned = " + returned);
            Console.ReadKey();

        }
    }
}