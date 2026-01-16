using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ONP
{
    public class IncorrectOperationStringException : Exception
    {
        public IncorrectOperationStringException(string message)
        : base(message)
        {
        }
    }
}