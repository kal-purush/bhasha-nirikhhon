using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleCalc
{
    public class Calc
    {
        public int Sub(int x, int y)
        {
            return x - y;
        }
        public int sqr(int x, int y)
        {
            return x * x;
        }
        public int umn(int x, int y)
        {
            return x * y;
        }
        public int del(int x, int y)
        {
            return x / y;
        }
        public int sum(int x, int y)
        {
            return x + y;
        }
    }
}