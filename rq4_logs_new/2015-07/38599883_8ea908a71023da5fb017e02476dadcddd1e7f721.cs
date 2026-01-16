using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace zadanie_1_2
{
    class Program
    {
        static void Main(string[] args)
        {
            double x, y, r;
            string inside = "точка внутри контура";
            string outside = "точка вне контура";
            Console.WriteLine("введите абсциссу точки: ");
            x = double.Parse(Console.ReadLine());
            Console.WriteLine("введите ординату точки: ");
            y = double.Parse(Console.ReadLine());
            r = Math.Sqrt(Math.Pow(x, 2) + Math.Pow(y, 2));
            if (x < 0 && y > 0 && r <= 1)
                Console.WriteLine(inside);
            if (x > 0 && y > 0 && r <= 1 && r >= 0.5)
                Console.WriteLine(inside);
            else
                Console.WriteLine(outside);

            Console.ReadKey();
        }
    }
}