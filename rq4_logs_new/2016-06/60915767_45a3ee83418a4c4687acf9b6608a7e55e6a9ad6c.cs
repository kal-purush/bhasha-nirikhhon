using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Projekt
{
    public class Alkohol
    {

        public int ilosc { get; set; }
        public double litraz { get; set; }
        public double woltaz { get; set; }
        public Trunek trunek { get; set; }


        public Alkohol(int ilosc, double litraz, double woltaz, Trunek trunek)
        {

            this.ilosc = ilosc;
            this.litraz = litraz;
            this.woltaz = woltaz;
            this.trunek = trunek;

        }


    }
}