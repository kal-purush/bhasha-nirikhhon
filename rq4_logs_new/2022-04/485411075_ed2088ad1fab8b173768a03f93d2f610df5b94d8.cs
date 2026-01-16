using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork3
{
    public class Complex
    {
        public double im;
        public double re;
     
        public Complex() { }

        public Complex(double im, double re)
        {
            this.im = im;
            this.re = re;
        }

        public Complex Plus(Complex b)
        {
            Complex a = new Complex
            {
                im = b.im + im,
                re = b.re + re
            };
            return a;
        }

        public Complex Minus(Complex b)
        {
            Complex a = new Complex
            {
                re = re - b.re,
                im = im - b.im
            };
            return a;
        }

        public Complex Multi(Complex b)
        {
            Complex a = new Complex
            {
                im = re * b.im + im * b.re,
                re = re * b.re - im * b.im
            };
            return a;
        }


        public string ToString() => re + "+" + im + "i";


    }
}