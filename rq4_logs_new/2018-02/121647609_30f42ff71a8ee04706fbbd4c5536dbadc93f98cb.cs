using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    class Faktoriyel
    {
        public int faktoriyelHesabi(int a)
        {
            int sonuc = 1;

            for (int i = a; i > 0; i--)
                sonuc *= a;

            return sonuc;

        }
    }
}