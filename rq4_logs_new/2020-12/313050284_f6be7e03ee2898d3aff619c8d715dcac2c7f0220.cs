using System;

namespace Calc
{
    public static class Calc
    {
       static public int Plus(int x,int y)
        {
            return x + y;
        }

        static public int Minus(int x,int y)
        {
            return x - y;
        }

        static public int Multiplication(int x,int y)
        {
            return x * y;
        }

        static public int Division(int x,int y)
        {
            if (y == 0)
                return 0;
            return x / y;
        }
    }
}