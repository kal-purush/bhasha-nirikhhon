using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MLPCore
{
    public class Results
    {
        public double X { get; private set; }
        public double Y { get; private set; }
        public int Class { get; private set; }

        public Results(double x, double y, int c)
        {
            X = x;
            Y = y;
            Class = c;
        }
    }
}