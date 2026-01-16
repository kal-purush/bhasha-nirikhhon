using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Codewars
{
    internal class Exe1
    {
        public int[] Number { get; set; }

        public Exe1(int[] number)
        {
            Number = number;
        }

        public int[] Calc()
        {
            var nQuery =
                from VrNum in Number
                where (VrNum % 2) == 0
                select VrNum;
            return nQuery.ToArray();


            //return Number.Where(x => x % 2 == 0).ToArray();
        }
    }
}