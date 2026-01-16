/*
Alec Hislop
P452289

PROGRAMMING III
Portfolio Activity 1.7 - Math Library

Contains classes for arithmetic, trigonometric and algebraic operations.
*/

using System;

namespace MathAlgebra
{
    public class Algebraic
    {
        public static double SquareRoot(double a)
        {
            return Math.Sqrt(a);
        }

        public static double CubeRoot(double a)
        {
            return Math.Pow(a, (1.0 / 3.0));
        }

        public static double Inverse(double a)
        {
            return (1.0 / a);
        }
    }
}