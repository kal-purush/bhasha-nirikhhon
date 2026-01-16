using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork3
{
    class Fraction
    {
        private int numerator;

        private int denumerator;

        public Fraction(){}

        public Fraction(int numerator, int denumerator)
        {
            this.numerator = numerator;
            this.denumerator = denumerator;
        }

        public Fraction Plus(Fraction x)
        {
            int resultNum;
            int resultDenum;
            if(denumerator == x.denumerator)
            {
                resultNum = numerator + x.numerator;
            }
            else
            {
                resultNum = numerator * x.denumerator + x.numerator * denumerator;
            }
            resultDenum = denumerator * x.denumerator;

            return new Fraction
            {
                numerator = resultNum,
                denumerator = resultDenum,
            };
        }

        public Fraction Minus(Fraction x)
        {
            int resultNum;
            int resultDenum;
            if (denumerator == x.denumerator)
            {
                resultNum = numerator - x.numerator;
            }
            else
            {
                resultNum = numerator * x.denumerator - x.numerator * denumerator;
            }
            resultDenum = denumerator * x.denumerator;

            return new Fraction
            {
                numerator = resultNum,
                denumerator = resultDenum,
            };
        }

    }
}