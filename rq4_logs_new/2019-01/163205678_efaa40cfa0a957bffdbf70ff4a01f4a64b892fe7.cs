using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EKlubas.Application
{
    public class FractionFigEqualityDto
    {
        public Fraction Fraction { get; set; }
        public Fraction TaskFraction { get; set; }

        public FractionFigEqualityDto() { }
        public FractionFigEqualityDto(Fraction fraction, Fraction taskFraction)
        {
            Fraction = fraction;
            TaskFraction = taskFraction;
        }
    }
}