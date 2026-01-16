using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EKlubas.Application
{
    public class Fraction
    {
        public int Numerator { get; set; }
        public int Denominator { get; set; }

        public Fraction()
        {

        }

        public Fraction(int Numerator, int Denominator)
        {
            this.Numerator = Numerator;
            this.Denominator = Denominator;
        }

        public decimal GetFractionDecimalForm()
        {
            decimal result = Numerator / Denominator;

            return Math.Round(result, 2);
        }

        public string GetFractionHtml()
        {
            return $"<sup>{Numerator}</sup>&frasl;<sub>{Denominator}</sub>";
        }
    }
}