using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Methods
{
    public class Lab05
    {
        public static String task5662(int a, int b, int c)
        {
            return String.Format("Уравнение {0}x^2 + {1}x + {2} имеет один корень", a, b, c);
        }

        public static bool task4847(int x, int y, int z)
        {
            bool r;
            r = x == y || y == z || x == z;
            return r;
        }
    }
}