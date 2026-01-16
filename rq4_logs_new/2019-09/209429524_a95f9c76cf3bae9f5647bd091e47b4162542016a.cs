using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test2a
{
    class Program
    {
        static void Main(string[] args)
        {
            //attempted object creation
            Salary.a1 = new Salary();
            //attempted calls and returns
            Console.WriteLine(a1.conss());
            Console.WriteLine(a1.DisplaySalary());
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine(b1.consw());
            //attempted object creation and initialisation
            Console.Write("Input hours worked: ");
            int Nhours = int.Parse(Console.ReadLine());
            Wages.b1 = new Wages(hours);
            Console.WriteLine(b1.DisplayWages());
            Console.ReadLine();
        }
    }
    class Salary
    {
        double annualsalary = 80000;
        double weeklysalary;

        string conss()
        {
            return "Your annual salary is " + annualsalary;
        }
        public string DisplaySalary()
        {
            weeklysalary = annualsalary / 52;
            return "Your weekly Salary is " + weeklysalary;
        }
    }
    class Wages
    {
        double hourlyrate = 33.72;
        int numhours = 0;
        double weeklywages = 0;
        public int Nhours { get { return numhours; } set { numhours = value; } }

        string consw()
            {
            return "I will calculate wages";
            }
        public hours(int _hours)
        {
            Nhours = _hours;
        }
        public string DisplayWages()
        {
            weeklywages = numhours * hourlyrate;
            return "Your weekly wages are " + weeklywages;
        }
    }
}