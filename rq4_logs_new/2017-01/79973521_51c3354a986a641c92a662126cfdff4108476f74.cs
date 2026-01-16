using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MathMagician.Numbers
{
    interface Integer
    {
        int GetFirst();
        int GetNext(int current);
        int GetSequence(int how_many);
        string printNumbers(int[] how_many);
    }
}