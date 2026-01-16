using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    class Operations
    { //  Todo: think of double return and params.
        public static int Add(long a, long b)
        {
            return (int)(a + b);
        }

        public static int Sub(long a, long b)
        {
            return (int)(a - b);
        }

        public static int Mult(long a, long b)
        {
            return (int)(a - b);
        }

        public static int Div(long a, long b)
        {
            return (int)(a / b);
        }

        public static int Mod(long a, long b)
        {
            return (int)(a % b);
        }
    }
}