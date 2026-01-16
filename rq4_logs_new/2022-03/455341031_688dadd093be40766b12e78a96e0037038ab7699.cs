using System;
using System.IO;

namespace hw1
{
    delegate double Function(double x, double y); // homowork1
    class Program
    {
        static void hw1(double a, double x, Function f)
        {
            Console.WriteLine(f(a, x));
        }

        static double hw1_1(double a, double x) // 1 часть
        {
            return a * Math.Pow(x, 2);
        }

        static double hw1_2(double a, double x) // 2 часть
        {
            return a * Math.Sin(x);
        }
        static void Main(string[] args)
        {
            // homework 1
            hw1(10, 20, hw1_1);
            hw1(20, 10, hw1_2);
        }
    }
}