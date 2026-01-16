using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AAA;

namespace Company
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Employee a = new Employee();
            Console.Write("請輸入員工名稱:");
            a.Name = Console.ReadLine();

            Console.Write("請輸入底薪:");
            try
            {
                int baseSalary = int.Parse(Console.ReadLine());
                a.BaseSalary = baseSalary;
                Console.WriteLine("這位員工的底薪為{0}", baseSalary);
            }
            catch
            {
                Console.WriteLine("薪水輸入錯誤");
                return;
            }

            Console.Write("請輸入獎金值");
            try
            {
                int benefit = int.Parse(Console.ReadLine());
                a.Benefit = benefit;
                Console.WriteLine("這位員工的獎金為{0}", benefit);
            }
            catch
            {
                Console.WriteLine("獎金輸入錯誤");
                return;
            }

            Console.Write("所以你這個月可以領:{0}", a.Salary);
        }
    }
}