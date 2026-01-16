using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibraryMathHelper
{
    public class MathHelper
    {
        //public double a;
        //public double b;
        //public MathHelper(double a, double b)
        //{
            //this.a = a;
            //this.b = b;
        //}
        public static double Summ(double a, double b)
        {
            return a + b;
        }
        public static double Sub(double a, double b)
        {
            return a - b;
        }
        public static double Mult(double a, double b)
        {
            return a * b;
        }
        public static double Div(double a, double b)
        {
            return a / b;
        }
    }
}