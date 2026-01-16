using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Algorithms.Implementation
{
    
    public class Bits
    {

        #region Addition

        public static int Add(int a, int b)
        {
            if (b == 0)
            {
                return a;
            }

            //if we do bit wise exclusive or, it will add with unqiue bits (only 1 if only one is 1)
            int result = a ^ b;

            //now we need to perform carry by doing bitwise and and shift one
            int carry = (a & b) << 1;

            //now continue to add result with carry (if carry causes another carry then we need to perform above operation
            return Add(result, carry);
        }

        #endregion

    }

}