using System;

namespace GB1Lesson3
{
    internal class Fraction
    {
        private readonly int numerator;
        private readonly int denominator;

        public Fraction(int numerator, int denominator)
        {
            if (denominator == 0)
            {
                throw new ArgumentException("Fraction denominator can't be zero");
            }

            (numerator, denominator) = SimplifyParts(numerator, denominator);

            this.numerator = numerator;
            this.denominator = denominator;
        }

        public int Numerator
        {
            get { return numerator; }
        }

        public int Denominator
        {
            get { return denominator; }
        }

        public double Double
        {
            get { return numerator / (double)denominator; }
        }

        public static Fraction operator +(Fraction left, Fraction right)
        {
            return Simplify(
                new Fraction(
                    left.numerator * right.denominator + left.denominator * right.numerator,
                    left.denominator * right.denominator
                )
            );
        }

        public static Fraction operator -(Fraction left, Fraction right)
        {
            int a = left.numerator * right.denominator - left.denominator * right.numerator;
            int b = left.denominator * right.denominator;
            return Simplify(
                new Fraction(
                    a,
                    b
                )
            );
        }

        public static Fraction operator *(Fraction left, Fraction right)
        {
            return Simplify(
                new Fraction(
                    left.numerator * right.numerator,
                    left.denominator * right.denominator
                )
            );
        }

        public static Fraction operator /(Fraction left, Fraction right)
        {
            return Simplify(
                new Fraction(
                    left.numerator * right.denominator,
                    left.denominator * right.numerator
                )
            );
        }

        public Fraction Sum(Fraction fraction)
        {
            return this + fraction;
        }

        public Fraction Sub(Fraction fraction)
        {
            return this - fraction;
        }

        public Fraction Mult(Fraction fraction)
        {
            return this * fraction;
        }

        public Fraction Div(Fraction fraction)
        {
            return this / fraction;
        }

        public static Fraction Simplify(Fraction fraction)
        {
            (int numerator, int denominator) = SimplifyParts(fraction.numerator, fraction.denominator);

            return new Fraction(numerator, denominator);
        }

        private static (int numerator, int denominator) SimplifyParts(int numerator, int denominator)
        {
            int GCD = (int)GetGCD((uint)Math.Abs(numerator), (uint)Math.Abs(denominator));

            return (numerator: numerator / GCD, denominator: denominator / GCD);
        }

        /// <summary>
        /// Calculate GCD. See https://en.wikipedia.org/wiki/Euclidean_algorithm
        /// </summary>
        /// <param name="a">number a</param>
        /// <param name="b">number b</param>
        /// <returns>greatest common devisor for a and b</returns>
        private static uint GetGCD(uint a, uint b)
        {
            while (a != 0 && b != 0)
            {
                if (a > b)
                {
                    a %= b;
                }
                else
                {
                    b %= a;
                }
            }

            return a == 0 ? b : a;
        }

        public override string ToString()
        {
            return numerator + "/" + denominator;
        }
    }
}