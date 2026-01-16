using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Giraffe
{
    public class FindMaxValue
    {
        public int GetMaxValue(int num1, int num2, int num3)
        {

            int result ;

            if (num1 >= num2 && num1 >= num3)
            {
                result = num1;
            }
            else if (num2 >= num1 && num2 >= num3){
                result = num2;
            }
            else
            {
                result = num3;
            }
            return result;
        }

    }
}