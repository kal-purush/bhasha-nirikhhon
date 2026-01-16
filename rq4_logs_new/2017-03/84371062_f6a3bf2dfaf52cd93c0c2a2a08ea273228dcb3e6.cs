using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Matricies.Classes
{
    public static class MathematicalHelper
    {
        #region Arithmetic Functions
        public static decimal Add(decimal left, decimal right)
        {
            return left + right;
        }

        public static decimal Substract(decimal left, decimal right)
        {
            return left - right;
        }

        public static decimal Multiply(decimal left, decimal right)
        {
            return left * right;
        }

        public static decimal Divide(decimal left, decimal right)
        {
            return left / right;
        }
    }
        #endregion
    
}