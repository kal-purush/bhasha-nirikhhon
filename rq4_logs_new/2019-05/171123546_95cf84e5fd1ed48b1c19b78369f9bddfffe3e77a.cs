using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    /// <summary>
    /// Throws when expression contains division by zero
    /// </summary>
    [Serializable]
    public class DivisionByZeroException : Exception
    {
        public DivisionByZeroException()
        {
        }

        public DivisionByZeroException(string message)
            : base(message)
        {
        }

        public DivisionByZeroException(string message, Exception inner)
            : base(message, inner)
        {
        }

        protected DivisionByZeroException(System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }
    }
}
