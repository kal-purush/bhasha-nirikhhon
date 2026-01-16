using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Example04
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.Write("請輸入第一個數字");
            try
            {
                int number1 = int.Parse(Console.ReadLine());
            }
            catch (Exception)
            Console.Write("請輸入第二個數字");
            int number2 = int.Parse(Console.ReadLine());
            Console.Write("{0} + {1} = {2}",number1,number2,number1 + number2);
        }
    }
}